use chrono::Local;
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

// MQTT protocol constants
const CONNECT: u8 = 1;
const CONNACK: u8 = 2;
const PUBLISH: u8 = 3;
const PUBACK: u8 = 4;
const SUBSCRIBE: u8 = 8;
const SUBACK: u8 = 9;
const PINGREQ: u8 = 12;
const PINGRESP: u8 = 13;
const DISCONNECT: u8 = 14;

const MQTT_PORT: u16 = 1883;

// Client connection info
struct Client {
    client_id: String,
    stream: TcpStream,
    subscriptions: Vec<String>,
}

impl Client {
    fn new(client_id: String, stream: TcpStream) -> Self {
        Client {
            client_id,
            stream,
            subscriptions: Vec::new(),
        }
    }
}

// Type to hold topic and message data
type TopicMessage = (String, Vec<u8>);

// Main broker state
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

    fn add_client(&mut self, client_id: String, stream: TcpStream) -> io::Result<()> {
        log(&format!("Adding new client: {}", client_id));
        let client = Client::new(client_id.clone(), stream);
        self.clients.insert(client_id, client);
        Ok(())
    }

    fn add_subscription(&mut self, client_id: &str, topic: String) -> io::Result<()> {
        if let Some(client) = self.clients.get_mut(client_id) {
            log(&format!(
                "Client '{}' subscribing to topic '{}'",
                client_id, topic
            ));
            client.subscriptions.push(topic);
        }
        Ok(())
    }

    fn queue_message(&mut self, topic: String, message: Vec<u8>) -> io::Result<()> {
        self.message_queue.push((topic, message));
        Ok(())
    }

    fn process_messages(&mut self) -> io::Result<()> {
        let mut delivered_messages = Vec::new();

        // Process each message in the queue
        for (index, (topic, message)) in self.message_queue.iter().enumerate() {
            // Keep track of clients we've delivered to
            let mut delivered = false;

            // For each client, check if they're subscribed to this topic
            for (client_id, client) in self.clients.iter_mut() {
                if client.subscriptions.iter().any(|t| topic_matches(t, topic)) {
                    // Deliver the message
                    match deliver_message(&mut client.stream, topic, message) {
                        Ok(_) => {
                            log(&format!(
                                "Delivered message on topic '{}' to client '{}'",
                                topic, client_id
                            ));
                            delivered = true;
                        }
                        Err(e) => {
                            log(&format!(
                                "Failed to deliver message to client '{}': {}",
                                client_id, e
                            ));
                            // Could handle client disconnection here
                        }
                    }
                }
            }

            // If we delivered to at least one client, mark for removal
            if delivered {
                delivered_messages.push(index);
            }
        }

        // Remove delivered messages from the queue (in reverse to avoid index shifting)
        for index in delivered_messages.iter().rev() {
            self.message_queue.remove(*index);
        }

        Ok(())
    }

    fn remove_client(&mut self, client_id: &str) -> io::Result<()> {
        if self.clients.remove(client_id).is_some() {
            log(&format!("Removed client: {}", client_id));
        }
        Ok(())
    }
}

fn main() -> io::Result<()> {
    log("MQTT Broker starting...");

    // Create a TCP listener on the MQTT port
    let listener = TcpListener::bind(format!("0.0.0.0:{}", MQTT_PORT))?;
    log(&format!("Broker listening on port {}", MQTT_PORT));

    // Share broker state across threads
    let broker = Arc::new(Mutex::new(Broker::new()));

    // Start a thread to process the message queue
    let broker_clone = broker.clone();
    thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_millis(100));
            if let Ok(mut broker) = broker_clone.lock() {
                if let Err(e) = broker.process_messages() {
                    log(&format!("Error processing messages: {}", e));
                }
            }
        }
    });

    // Accept and handle client connections
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let peer_addr = stream.peer_addr()?;
                log(&format!("New connection from: {}", peer_addr));

                // Clone broker state for the client handler thread
                let broker_clone = broker.clone();

                thread::spawn(move || {
                    if let Err(e) = handle_client(stream, broker_clone) {
                        log(&format!("Client error: {}", e));
                    }
                });
            }
            Err(e) => {
                log(&format!("Connection error: {}", e));
            }
        }
    }

    Ok(())
}

fn handle_client(mut stream: TcpStream, broker: Arc<Mutex<Broker>>) -> io::Result<()> {
    // Set timeouts for read operations
    stream.set_read_timeout(Some(Duration::from_secs(120)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;

    // Keep track of client ID
    let mut client_id = String::new();

    // Process client packets
    loop {
        let mut fixed_header = [0u8; 1];

        // Read the fixed header
        match stream.read_exact(&mut fixed_header) {
            Ok(_) => {
                let packet_type = fixed_header[0] >> 4;
                let rem_len = read_remaining_length(&mut stream)?;
                let mut payload = vec![0u8; rem_len];
                stream.read_exact(&mut payload)?;

                match packet_type {
                    CONNECT => {
                        // Extract client ID from CONNECT packet
                        let id = extract_client_id(&payload)?;
                        client_id = id.clone();

                        // Send CONNACK
                        send_connack(&mut stream)?;

                        // Add client to broker
                        if let Ok(mut broker) = broker.lock() {
                            broker.add_client(id, stream.try_clone()?)?;
                        }
                    }
                    PUBLISH => {
                        // Extract topic and message from PUBLISH packet
                        let (topic, message) = extract_publish_data(&payload)?;
                        log(&format!(
                            "Received publish on topic '{}': {:?}",
                            topic, message
                        ));

                        // Queue message for delivery
                        if let Ok(mut broker) = broker.lock() {
                            broker.queue_message(topic, message)?;
                        }
                    }
                    SUBSCRIBE => {
                        // Extract subscription topics
                        let topics = extract_subscribe_topics(&payload)?;
                        let packet_id = ((payload[0] as u16) << 8) | payload[1] as u16;

                        // Add subscriptions for this client
                        if let Ok(mut broker) = broker.lock() {
                            for topic in &topics {
                                broker.add_subscription(&client_id, topic.to_string())?;
                            }
                        }

                        // Send SUBACK
                        send_suback(&mut stream, packet_id, topics.len())?;
                    }
                    PINGREQ => {
                        // Respond to ping request
                        send_pingresp(&mut stream)?;
                    }
                    DISCONNECT => {
                        log(&format!("Client '{}' disconnected", client_id));

                        // Remove client from broker
                        if let Ok(mut broker) = broker.lock() {
                            broker.remove_client(&client_id)?;
                        }

                        return Ok(());
                    }
                    _ => {
                        log(&format!("Unhandled packet type: {}", packet_type));
                    }
                }
            }
            Err(e) => {
                // Handle disconnection
                if !client_id.is_empty() {
                    log(&format!("Client '{}' connection error: {}", client_id, e));

                    // Remove client from broker
                    if let Ok(mut broker) = broker.lock() {
                        broker.remove_client(&client_id)?;
                    }
                }
                return Err(e);
            }
        }
    }
}

// Extract client ID from CONNECT packet
fn extract_client_id(payload: &[u8]) -> io::Result<String> {
    // Protocol name length (should be 4 for "MQTT")
    let protocol_len = ((payload[0] as u16) << 8) | payload[1] as u16;

    // Skip protocol name and version + flags
    let client_id_len_pos = 2 + protocol_len as usize + 2;

    // Client ID length
    let client_id_len =
        ((payload[client_id_len_pos] as u16) << 8) | payload[client_id_len_pos + 1] as u16;

    // Extract client ID
    let client_id_start = client_id_len_pos + 2;
    let client_id_end = client_id_start + client_id_len as usize;

    if client_id_end <= payload.len() {
        let client_id =
            String::from_utf8_lossy(&payload[client_id_start..client_id_end]).to_string();
        Ok(client_id)
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid client ID",
        ))
    }
}

// Extract topic and message from PUBLISH packet
fn extract_publish_data(payload: &[u8]) -> io::Result<(String, Vec<u8>)> {
    // Topic length
    let topic_len = ((payload[0] as u16) << 8) | payload[1] as u16;

    // Extract topic
    let topic_start = 2;
    let topic_end = topic_start + topic_len as usize;

    if topic_end <= payload.len() {
        let topic = String::from_utf8_lossy(&payload[topic_start..topic_end]).to_string();

        // Extract message (everything after the topic)
        let message = payload[topic_end..].to_vec();

        Ok((topic, message))
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid publish data",
        ))
    }
}

// Extract subscription topics from SUBSCRIBE packet
fn extract_subscribe_topics(payload: &[u8]) -> io::Result<Vec<String>> {
    let mut topics = Vec::new();
    let mut pos = 2; // Skip packet ID

    while pos < payload.len() {
        // Topic length
        if pos + 2 > payload.len() {
            break;
        }
        let topic_len = ((payload[pos] as u16) << 8) | payload[pos + 1] as u16;
        pos += 2;

        // Topic string
        if pos + topic_len as usize > payload.len() {
            break;
        }
        let topic = String::from_utf8_lossy(&payload[pos..pos + topic_len as usize]).to_string();
        topics.push(topic);
        pos += topic_len as usize;

        // QoS
        if pos < payload.len() {
            pos += 1;
        }
    }

    Ok(topics)
}

// Check if a subscription topic matches a published topic
fn topic_matches(subscription: &str, published: &str) -> bool {
    // Exact match
    if subscription == published {
        return true;
    }

    // Simple wildcard handling
    if subscription.ends_with("/#") {
        let prefix = &subscription[0..subscription.len() - 2];
        return published.starts_with(prefix);
    }

    // For more complex wildcard handling, extend this function

    false
}

// Send CONNACK packet
fn send_connack(stream: &mut TcpStream) -> io::Result<()> {
    let connack = [0x20, 0x02, 0x00, 0x00]; // CONNACK, length 2, no session, accepted
    stream.write_all(&connack)?;
    Ok(())
}

// Send SUBACK packet
fn send_suback(stream: &mut TcpStream, packet_id: u16, topic_count: usize) -> io::Result<()> {
    let mut suback = vec![0x90, 2 + topic_count as u8]; // SUBACK, length = 2 + topic_count
    suback.push((packet_id >> 8) as u8);
    suback.push((packet_id & 0xFF) as u8);

    // QoS 0 for all subscriptions
    for _ in 0..topic_count {
        suback.push(0x00);
    }

    stream.write_all(&suback)?;
    Ok(())
}

// Send PINGRESP packet
fn send_pingresp(stream: &mut TcpStream) -> io::Result<()> {
    let pingresp = [0xD0, 0x00]; // PINGRESP, length 0
    stream.write_all(&pingresp)?;
    Ok(())
}

// Deliver a message to a subscribed client
fn deliver_message(stream: &mut TcpStream, topic: &str, message: &[u8]) -> io::Result<()> {
    let topic_bytes = topic.as_bytes();
    let topic_len = topic_bytes.len();

    // Calculate packet size
    let variable_header_size = 2 + topic_len; // 2 for topic length + topic
    let payload_size = message.len();
    let total_size = variable_header_size + payload_size;

    // Create publish packet
    let mut packet = Vec::with_capacity(1 + 4 + total_size); // Estimate max size

    // Fixed header
    packet.push(0x30); // PUBLISH, QoS 0, no retain

    // Remaining length (variable encoding)
    let rem_len = encode_remaining_length(total_size);
    packet.extend(rem_len);

    // Topic length
    packet.push((topic_len >> 8) as u8);
    packet.push((topic_len & 0xFF) as u8);

    // Topic
    packet.extend_from_slice(topic_bytes);

    // Payload
    packet.extend_from_slice(message);

    // Send packet
    stream.write_all(&packet)?;
    Ok(())
}

// Read remaining length with variable encoding
fn read_remaining_length(stream: &mut TcpStream) -> io::Result<usize> {
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
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Malformed remaining length",
            ));
        }
    }
    Ok(value)
}

// Encode remaining length with variable encoding
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

// Logging function with timestamp
fn log(msg: &str) {
    let now = Local::now().format("%Y-%m-%d %H:%M:%S");
    println!("[{}] {}", now, msg);
}
