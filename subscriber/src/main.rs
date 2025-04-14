use chrono::Local;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

const MQTT_BROKER: &str = "broker.hivemq.com";
const MQTT_PORT: u16 = 1883;
const PING_INTERVAL: Duration = Duration::from_secs(30);
const SENSOR_ID: &str = "room_sensor_livingroom";

fn main() -> std::io::Result<()> {
    log("Smart Home Display starting...");
    let mut stream = TcpStream::connect((MQTT_BROKER, MQTT_PORT))?;
    stream.set_read_timeout(Some(Duration::from_secs(60)))?;
    log(&format!(
        "Connected to MQTT broker: {}:{}",
        MQTT_BROKER, MQTT_PORT
    ));

    let connect = connect_packet("display_livingroom");
    stream.write_all(&connect)?;
    log("Sent CONNECT packet");

    let mut connack = [0u8; 4];
    stream.read_exact(&mut connack)?;
    log(&format!("Received CONNACK: {:02X?}", connack));

    if connack[3] != 0 {
        log(&format!("Connection refused with code: {}", connack[3]));
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Connection refused",
        ));
    }

    let topic = format!("home/{}/temperature_humidity", SENSOR_ID);
    let subscribe = subscribe_packet(1, &topic, 0);
    stream.write_all(&subscribe)?;
    log(&format!("Sent SUBSCRIBE packet for topic '{}'", topic));

    let mut suback = [0u8; 5];
    stream.read_exact(&mut suback)?;
    log(&format!("Received SUBACK: {:02X?}", suback));

    log("Waiting for temperature and humidity data...");
    let mut last_ping = Instant::now();

    loop {
        stream.set_read_timeout(Some(Duration::from_secs(1)))?;

        if last_ping.elapsed() >= PING_INTERVAL {
            log("Sending ping to keep connection alive");
            stream.write_all(&ping_packet())?;
            last_ping = Instant::now();
        }

        let mut fixed_header = [0u8; 1];
        match stream.read(&mut fixed_header) {
            Ok(1) => {
                let packet_type = fixed_header[0] >> 4;

                if packet_type == 3 {
                    // PUBLISH
                    let rem_len = read_remaining_length(&mut stream)?;
                    let mut publish_payload = vec![0; rem_len];
                    stream.read_exact(&mut publish_payload)?;
                    let (received_topic, message) = decode_publish_payload(&publish_payload);
                    if received_topic == topic {
                        log(&format!(
                            "Received data on '{}': {}",
                            received_topic, message
                        ));
                    } else {
                        log(&format!(
                            "Received data on unexpected topic '{}': {}",
                            received_topic, message
                        ));
                    }
                } else if packet_type == 13 {
                    // PINGRESP
                    let rem_len = read_remaining_length(&mut stream)?;
                    if rem_len > 0 {
                        let mut pingresp_payload = vec![0; rem_len];
                        stream.read_exact(&mut pingresp_payload)?;
                    }
                    log("Received PINGRESP");
                } else {
                    log(&format!("Received unknown packet type: {}", packet_type));
                    let rem_len = read_remaining_length(&mut stream)?;
                    let mut payload = vec![0; rem_len];
                    stream.read_exact(&mut payload)?;
                }
            }
            Ok(0) => {
                log("Connection closed by the broker.");
                break;
            }
            Ok(_) => {
                log("Received more than 1 byte in fixed header - unexpected.");
                break;
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut
                {
                    continue;
                } else {
                    log(&format!("Error reading from socket: {}", e));
                    break;
                }
            }
        }
    }

    Ok(())
}

fn connect_packet(client_id: &str) -> Vec<u8> {
    let mut packet = vec![0x10, 0, 0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 60];
    let client_id_bytes = client_id.as_bytes();
    packet.push((client_id_bytes.len() >> 8) as u8);
    packet.push((client_id_bytes.len() & 0xFF) as u8);
    packet.extend_from_slice(client_id_bytes);
    packet[1] = (packet.len() - 2) as u8;
    packet
}

fn subscribe_packet(packet_id: u16, topic: &str, qos: u8) -> Vec<u8> {
    let mut packet = vec![0x82];
    let mut vh_payload = Vec::new();
    vh_payload.push((packet_id >> 8) as u8);
    vh_payload.push((packet_id & 0xFF) as u8);
    let topic_bytes = topic.as_bytes();
    vh_payload.push((topic_bytes.len() >> 8) as u8);
    vh_payload.push((topic_bytes.len() & 0xFF) as u8);
    vh_payload.extend_from_slice(topic_bytes);
    vh_payload.push(qos);
    let rem_len = encode_remaining_length(vh_payload.len());
    packet.extend(rem_len);
    packet.extend(vh_payload);
    packet
}

fn ping_packet() -> Vec<u8> {
    vec![0xC0, 0x00]
}

fn encode_remaining_length(mut length: usize) -> Vec<u8> {
    let mut encoded_bytes = Vec::new();
    loop {
        let mut byte = (length % 128) as u8;
        length /= 128;
        if length > 0 {
            byte |= 0x80;
        }
        encoded_bytes.push(byte);
        if length == 0 {
            break;
        }
    }
    encoded_bytes
}

fn read_remaining_length(stream: &mut TcpStream) -> std::io::Result<usize> {
    let mut multiplier = 1;
    let mut value = 0;
    loop {
        let mut encoded_byte = [0u8; 1];
        stream.read_exact(&mut encoded_byte)?;
        let byte = encoded_byte[0];
        value += ((byte & 127) as usize) * multiplier;
        if (byte & 128) == 0 {
            break;
        }
        multiplier *= 128;
        if multiplier > 128 * 128 * 128 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Malformed remaining length",
            ));
        }
    }
    Ok(value)
}

fn decode_publish_payload(payload: &[u8]) -> (String, String) {
    let topic_len = u16::from_be_bytes([payload[0], payload[1]]) as usize;
    let topic = String::from_utf8_lossy(&payload[2..2 + topic_len]).to_string();
    let message = String::from_utf8_lossy(&payload[2 + topic_len..]).to_string();
    (topic, message)
}

fn log(msg: &str) {
    let now = Local::now().format("%Y-%m-%d %H:%M:%S");
    println!("[{}] {}", now, msg);
}
