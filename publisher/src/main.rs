use chrono::Local;
use rand::Rng;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

const MQTT_BROKER: &str = "127.0.0.1";
const MQTT_PORT: u16 = 1883;
const SENSOR_ID: &str = "room_sensor_livingroom";

fn main() -> std::io::Result<()> {
    log(&format!(
        "Temperature/Humidity Sensor '{}' starting…",
        SENSOR_ID
    ));

    let mut stream = TcpStream::connect((MQTT_BROKER, MQTT_PORT))?;
    stream.set_read_timeout(Some(Duration::from_secs(1)))?;
    log(&format!(
        "Connected to broker at {}:{}",
        MQTT_BROKER, MQTT_PORT
    ));

    // → CONNECT
    let connect = connect_packet(SENSOR_ID);
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

    let topic = format!("home/{}/temperature_humidity", SENSOR_ID);
    let mut rng = rand::rng();

    loop {
        let temperature: f32 = rng.random_range(18.0..28.0);
        let humidity: f32 = rng.random_range(30.0..70.0);
        let msg = format!(
            "{{\"temperature\": {:.2}, \"humidity\": {:.2}}}",
            temperature, humidity
        );

        let publish = publish_packet(&topic, &msg);
        stream.write_all(&publish)?;
        log(&format!("Published → {}", msg));

        handle_incoming(&mut stream)?;
        thread::sleep(Duration::from_secs(5));
    }
}

fn connect_packet(client_id: &str) -> Vec<u8> {
    let mut pkt = vec![0x10, 0, 0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 60];
    let idb = client_id.as_bytes();
    pkt.push((idb.len() >> 8) as u8);
    pkt.push((idb.len() & 0xFF) as u8);
    pkt.extend_from_slice(idb);
    pkt[1] = (pkt.len() - 2) as u8; // remaining length
    pkt
}

fn publish_packet(topic: &str, message: &str) -> Vec<u8> {
    let mut pkt = vec![0x30]; // PUBLISH, QoS 0
    let tb = topic.as_bytes();
    let mb = message.as_bytes();

    let mut vh = Vec::new();
    vh.push((tb.len() >> 8) as u8);
    vh.push((tb.len() & 0xFF) as u8);
    vh.extend_from_slice(tb);
    vh.extend_from_slice(mb);

    pkt.extend(encode_remaining_length(vh.len()));
    pkt.extend(vh);
    pkt
}

fn handle_incoming(stream: &mut TcpStream) -> std::io::Result<()> {
    loop {
        let mut fh = [0u8; 1];
        match stream.read(&mut fh) {
            Ok(1) => {
                let packet_type = fh[0] >> 4;
                let rem_len = read_remaining_length(stream)?;
                let mut buf = vec![0u8; rem_len];
                stream.read_exact(&mut buf)?;
                if packet_type == 13 {
                    log("Received PINGRESP");
                }
            }
            Ok(0) => break, // closed
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) => return Err(e),
            _ => {}
        }
    }
    Ok(())
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
