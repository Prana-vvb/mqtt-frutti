use chrono::Local;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

const MQTT_BROKER: &str = "127.0.0.1";
const MQTT_PORT: u16 = 1883;
const PING_INTERVAL: Duration = Duration::from_secs(30);
const SENSOR_ID: &str = "room_sensor_livingroom";

fn main() -> std::io::Result<()> {
    log("Smart Home Display starting…");
    let mut stream = TcpStream::connect((MQTT_BROKER, MQTT_PORT))?;
    stream.set_read_timeout(Some(Duration::from_secs(60)))?;
    log(&format!(
        "Connected to broker at {}:{}",
        MQTT_BROKER, MQTT_PORT
    ));

    // → CONNECT
    let client_id = "display_livingroom";
    let connect = connect_packet(client_id);
    stream.write_all(&connect)?;
    log("Sent CONNECT");

    // ← CONNACK
    let mut connack = [0u8; 4];
    stream.read_exact(&mut connack)?;
    log(&format!("Received CONNACK: {:02X?}", connack));
    if connack[3] != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Connection refused, code = {}", connack[3]),
        ));
    }

    // → SUBSCRIBE
    let topic = format!("home/{}/temperature_humidity", SENSOR_ID);
    let subscribe = subscribe_packet(1, &topic, 0);
    stream.write_all(&subscribe)?;
    log(&format!("Sent SUBSCRIBE for '{}'", topic));

    // ← SUBACK
    let mut fh = [0u8; 1];
    stream.read_exact(&mut fh)?;
    if fh[0] >> 4 == 9 {
        let rem = read_remaining_length(&mut stream)?;
        let mut payload = vec![0; rem];
        stream.read_exact(&mut payload)?;
        log(&format!("Received SUBACK: {:?}", payload));
    }

    // loop: PINGREQ + handle PUBLISH/PINGRESP
    let mut last_ping = Instant::now();
    loop {
        if last_ping.elapsed() >= PING_INTERVAL {
            stream.write_all(&ping_packet())?;
            log("Sent PINGREQ");
            last_ping = Instant::now();
        }

        stream.set_read_timeout(Some(Duration::from_millis(100)))?;
        if let Ok(1) = stream.read(&mut fh) {
            let packet_type = fh[0] >> 4;
            let rem = read_remaining_length(&mut stream)?;
            let mut buf = vec![0; rem];
            stream.read_exact(&mut buf)?;
            if packet_type == 3 {
                let (t, m) = decode_publish_payload(&buf);
                log(&format!("Received → '{}' = {}", t, m));
            } else if packet_type == 13 {
                log("Received PINGRESP");
            }
        }
    }
}

fn connect_packet(client_id: &str) -> Vec<u8> {
    let mut pkt = vec![0x10, 0, 0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 60];
    let idb = client_id.as_bytes();
    pkt.push((idb.len() >> 8) as u8);
    pkt.push((idb.len() & 0xFF) as u8);
    pkt.extend_from_slice(idb);
    pkt[1] = (pkt.len() - 2) as u8;
    pkt
}

fn subscribe_packet(packet_id: u16, topic: &str, qos: u8) -> Vec<u8> {
    let mut pkt = vec![0x82];
    let mut vh = Vec::new();
    vh.push((packet_id >> 8) as u8);
    vh.push((packet_id & 0xFF) as u8);
    let tb = topic.as_bytes();
    vh.push((tb.len() >> 8) as u8);
    vh.push((tb.len() & 0xFF) as u8);
    vh.extend_from_slice(tb);
    vh.push(qos);
    pkt.extend(encode_remaining_length(vh.len()));
    pkt.extend(vh);
    pkt
}

fn ping_packet() -> Vec<u8> {
    vec![0xC0, 0x00]
}

fn read_remaining_length(stream: &mut TcpStream) -> std::io::Result<usize> {
    let mut multiplier = 1;
    let mut value = 0;
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte)?;
        value += ((byte[0] & 0x7F) as usize) * multiplier;
        if byte[0] & 0x80 == 0 {
            break;
        }
        multiplier *= 128;
    }
    Ok(value)
}

fn decode_publish_payload(payload: &[u8]) -> (String, String) {
    let tlen = ((payload[0] as u16) << 8) | payload[1] as u16;
    let topic = String::from_utf8_lossy(&payload[2..2 + tlen as usize]).into();
    let msg = String::from_utf8_lossy(&payload[2 + tlen as usize..]).into();
    (topic, msg)
}

fn encode_remaining_length(mut x: usize) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut b = (x % 128) as u8;
        x /= 128;
        if x > 0 {
            b |= 0x80;
        }
        out.push(b);
        if x == 0 {
            break;
        }
    }
    out
}

fn log(msg: &str) {
    let t = Local::now().format("%Y-%m-%d %H:%M:%S");
    println!("[{}] {}", t, msg);
}
