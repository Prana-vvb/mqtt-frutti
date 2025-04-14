use chrono::Local;
use rand::Rng;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

const MQTT_BROKER: &str = "broker.hivemq.com";
const MQTT_PORT: u16 = 1883;
const SENSOR_ID: &str = "room_sensor_livingroom";

fn main() -> std::io::Result<()> {
    log(&format!(
        "Temperature/Humidity Sensor '{}' starting...",
        SENSOR_ID
    ));
    let mut stream = TcpStream::connect((MQTT_BROKER, MQTT_PORT))?;
    stream.set_read_timeout(Some(Duration::from_secs(1)))?;
    log(&format!(
        "Connected to MQTT broker: {}:{}",
        MQTT_BROKER, MQTT_PORT
    ));

    let connect = connect_packet(SENSOR_ID);
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
    let mut rng = rand::thread_rng();

    log(&format!("Publishing data to topic '{}'...", topic));
    loop {
        let temperature: f32 = rng.gen_range(18.0..28.0);
        let humidity: f32 = rng.gen_range(30.0..70.0);
        let message = format!(
            "{{\"temperature\": {:.2}, \"humidity\": {:.2}}}",
            temperature, humidity
        );

        let publish = publish_packet(&topic, &message, 0);
        stream.write_all(&publish)?;
        log(&format!("Published: {}", message));

        handle_incoming_packets(&mut stream)?;

        thread::sleep(Duration::from_secs(5));
    }
}

fn handle_incoming_packets(stream: &mut TcpStream) -> std::io::Result<()> {
    loop {
        let mut fixed_header = [0u8; 1];
        match stream.read_exact(&mut fixed_header) {
            Ok(_) => {
                let packet_type = fixed_header[0] >> 4;
                let rem_len = read_remaining_length(stream)?;
                let mut payload = vec![0; rem_len];
                stream.read_exact(&mut payload)?;

                if packet_type == 13 {
                    // PINGRESP
                    log("Received PINGRESP");
                } else {
                    log(&format!("Received packet type: {}", packet_type));
                }
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut
                {
                    break;
                } else {
                    return Err(e);
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

fn publish_packet(topic: &str, message: &str, qos: u8) -> Vec<u8> {
    let packet_type = 0x30 | (qos << 1);
    let mut packet = vec![packet_type];

    let topic_bytes = topic.as_bytes();
    let message_bytes = message.as_bytes();

    let mut variable_header_and_payload = Vec::new();
    variable_header_and_payload.push((topic_bytes.len() >> 8) as u8);
    variable_header_and_payload.push((topic_bytes.len() & 0xFF) as u8);
    variable_header_and_payload.extend_from_slice(topic_bytes);
    variable_header_and_payload.extend_from_slice(message_bytes);

    let rem_len = encode_remaining_length(variable_header_and_payload.len());
    packet.extend(rem_len);
    packet.extend(variable_header_and_payload);

    packet
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

fn log(msg: &str) {
    let now = Local::now().format("%Y-%m-%d %H:%M:%S");
    println!("[{}] {}", now, msg);
}
