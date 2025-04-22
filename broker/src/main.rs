use chrono::Local;
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

// MQTT packet types
const CONNECT: u8 = 1;
const PUBLISH: u8 = 3;
const SUBSCRIBE: u8 = 8;
const PINGREQ: u8 = 12;
const DISCONNECT: u8 = 14;
const MQTT_PORT: u16 = 1883;

struct Client {
    stream: TcpStream,
    subscriptions: Vec<String>,
}

impl Client {
    fn new(stream: TcpStream) -> Self {
        Client {
            stream,
            subscriptions: Vec::new(),
        }
    }
}

type TopicMessage = (String, Vec<u8>);

struct Broker {
    clients: HashMap<String, Client>,
    message_queue: Vec<TopicMessage>,
}

impl Broker {
    fn new() -> Self {
        Broker {
            clients: HashMap::new(),
            message_queue: Vec::new(),
        }
    }

    fn add_client(&mut self, id: String, stream: TcpStream) {
        log(&format!("Adding client: {}", id));
        self.clients.insert(id, Client::new(stream));
    }

    fn remove_client(&mut self, id: &str) {
        self.clients.remove(id);
        log(&format!("Removed client: {}", id));
    }

    fn add_subscription(&mut self, id: &str, topic: String) {
        if let Some(c) = self.clients.get_mut(id) {
            log(&format!("'{}' subscribes to '{}'", id, topic));
            c.subscriptions.push(topic);
        }
    }

    fn queue_message(&mut self, topic: String, payload: Vec<u8>) {
        self.message_queue.push((topic, payload));
    }

    fn process_messages(&mut self) {
        let mut to_remove = Vec::new();
        for (idx, (topic, msg)) in self.message_queue.iter().enumerate() {
            let mut delivered = false;
            for (id, client) in self.clients.iter_mut() {
                if client
                    .subscriptions
                    .iter()
                    .any(|sub| topic_matches(sub, topic))
                    && deliver_message(&mut client.stream, topic, msg).is_ok()
                {
                    log(&format!("Delivered '{}' → {}", topic, id));
                    delivered = true;
                }
            }
            if delivered {
                to_remove.push(idx);
            }
        }
        for &i in to_remove.iter().rev() {
            self.message_queue.remove(i);
        }
    }
}

fn main() -> io::Result<()> {
    log("MQTT Broker starting...");
    let listener = TcpListener::bind(("0.0.0.0", MQTT_PORT))?;
    log(&format!("Listening on port {}", MQTT_PORT));

    let broker = Arc::new(Mutex::new(Broker::new()));
    {
        let b = Arc::clone(&broker);
        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_millis(100));
                if let Ok(mut br) = b.lock() {
                    br.process_messages();
                }
            }
        });
    }

    for stream in listener.incoming() {
        let stream = stream?;
        let peer = stream.peer_addr()?;
        log(&format!("New connection from {}", peer));

        let b = Arc::clone(&broker);
        thread::spawn(move || {
            if let Err(e) = handle_client(stream, b) {
                log(&format!("Client thread error: {}", e));
            }
        });
    }
    Ok(())
}

fn handle_client(mut stream: TcpStream, broker: Arc<Mutex<Broker>>) -> io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(120)))?;
    let mut client_id = String::new();

    loop {
        let mut fh = [0u8; 1];
        stream.read_exact(&mut fh)?;
        let packet_type = fh[0] >> 4;
        let rem_len = read_remaining_length(&mut stream)?;
        let mut payload = vec![0; rem_len];
        stream.read_exact(&mut payload)?;

        match packet_type {
            CONNECT => {
                let id = extract_client_id(&payload)?;
                client_id = id.clone();
                send_connack(&mut stream)?;
                broker.lock().unwrap().add_client(id, stream.try_clone()?);
            }
            PUBLISH => {
                let (topic, msg) = extract_publish_data(&payload)?;
                broker.lock().unwrap().queue_message(topic, msg);
            }
            SUBSCRIBE => {
                let pid = ((payload[0] as u16) << 8) | payload[1] as u16;
                let topics = extract_subscribe_topics(&payload)?;
                for t in &topics {
                    broker
                        .lock()
                        .unwrap()
                        .add_subscription(&client_id, t.clone());
                }
                let _ = send_suback(&mut stream, pid, topics.len());
            }
            PINGREQ => {
                send_pingresp(&mut stream)?;
            }
            DISCONNECT => {
                broker.lock().unwrap().remove_client(&client_id);
                return Ok(());
            }
            _ => {}
        }
    }
}

fn read_remaining_length(stream: &mut TcpStream) -> io::Result<usize> {
    let mut multiplier = 1;
    let mut value = 0;
    loop {
        let mut b = [0u8; 1];
        stream.read_exact(&mut b)?;
        value += ((b[0] & 127) as usize) * multiplier;
        if b[0] & 128 == 0 {
            break;
        }
        multiplier *= 128;
    }
    Ok(value)
}

fn extract_client_id(payload: &[u8]) -> io::Result<String> {
    let proto_len = ((payload[0] as u16) << 8) | payload[1] as u16;
    let id_len_pos = 2 + proto_len as usize + 1 + 1 + 2;
    if id_len_pos + 2 > payload.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Malformed CONNECT",
        ));
    }
    let id_len = ((payload[id_len_pos] as u16) << 8) | payload[id_len_pos + 1] as u16;
    let start = id_len_pos + 2;
    let end = start + id_len as usize;
    if end > payload.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Malformed CONNECT",
        ));
    }
    Ok(String::from_utf8_lossy(&payload[start..end]).into())
}

fn send_connack(stream: &mut TcpStream) -> io::Result<()> {
    stream.write_all(&[0x20, 0x02, 0x00, 0x00])
}

fn extract_publish_data(payload: &[u8]) -> io::Result<(String, Vec<u8>)> {
    let tlen = ((payload[0] as u16) << 8) | payload[1] as u16;
    let end = 2 + tlen as usize;
    if end > payload.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Malformed PUBLISH",
        ));
    }
    let topic = String::from_utf8_lossy(&payload[2..end]).into();
    Ok((topic, payload[end..].to_vec()))
}

fn extract_subscribe_topics(payload: &[u8]) -> io::Result<Vec<String>> {
    let mut pos = 2;
    let mut out = Vec::new();

    while pos + 3 <= payload.len() {
        let tlen = ((payload[pos] as u16) << 8) | payload[pos + 1] as u16;
        pos += 2;
        if pos + tlen as usize + 1 > payload.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Malformed SUBSCRIBE",
            ));
        }
        let topic = String::from_utf8_lossy(&payload[pos..pos + tlen as usize]).into();
        pos += tlen as usize;
        let _qos = payload[pos];
        pos += 1;
        out.push(topic);
    }

    Ok(out)
}

fn send_suback(stream: &mut TcpStream, pid: u16, count: usize) -> io::Result<()> {
    let mut pkt = vec![0x90, (2 + count) as u8];
    pkt.push((pid >> 8) as u8);
    pkt.push(pid as u8);
    pkt.extend(std::iter::repeat_n(0x00, count));
    stream.write_all(&pkt)
}

fn send_pingresp(stream: &mut TcpStream) -> io::Result<()> {
    stream.write_all(&[0xD0, 0x00])
}

fn topic_matches(sub: &str, pub_t: &str) -> bool {
    if sub == pub_t {
        return true;
    }
    if let Some(prefix) = sub.strip_suffix("/#") {
        return pub_t.starts_with(prefix);
    }
    false
}

fn deliver_message(stream: &mut TcpStream, topic: &str, message: &[u8]) -> io::Result<()> {
    let tb = topic.as_bytes();
    let size = 2 + tb.len() + message.len();
    let mut pkt = Vec::with_capacity(1 + 4 + size);
    pkt.push(0x30);
    pkt.extend(encode_rem_len(size));
    pkt.push(((tb.len() as u16) >> 8) as u8);
    pkt.push((tb.len() as u16) as u8);
    pkt.extend(tb);
    pkt.extend(message);
    stream.write_all(&pkt)
}

fn encode_rem_len(mut x: usize) -> Vec<u8> {
    let mut o = Vec::new();
    loop {
        let mut b = (x % 128) as u8;
        x /= 128;
        if x > 0 {
            b |= 0x80;
        }
        o.push(b);
        if x == 0 {
            break;
        }
    }
    o
}

fn log(msg: &str) {
    let now = Local::now().format("%Y-%m-%d %H:%M:%S");
    println!("[{}] {}", now, msg);
}
