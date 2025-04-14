# RustyProtocols

A simple IoT system that simulates temperature and humidity sensing and display using MQTT protocol communication.

## Overview

This project consists of two Rust applications:

1. **Publisher** - Simulates a temperature/humidity sensor that publishes readings to an MQTT broker
2. **Subscriber** - Simulates a display device that subscribes to sensor data from the MQTT broker

The applications communicate via the public HiveMQ MQTT broker (`broker.hivemq.com`), making this system easy to run without setting up your own MQTT server.

## System Architecture

```
┌────────────┐         ┌──────────────────┐         ┌─────────────┐
│  Publisher │ ------> │  MQTT Broker     │ ------> │ Subscriber  │
│  (Sensor)  │         │ (HiveMQ Public)  │         │ (Display)   │
└────────────┘         └──────────────────┘         └─────────────┘
```

- **Publisher**: Generates random temperature (18-28°C) and humidity (30-70%) readings every 5 seconds
- **MQTT Broker**: Routes messages between publisher and subscriber based on topic subscriptions
- **Subscriber**: Receives and displays the sensor readings in real-time

## Installation & Usage

1. Clone this repository:
   ```
   git clone https://github.com/Prana-vvb/mqtt-frutti.git
   cd mqtt-frutti
   ```

2. Build and run the publisher (sensor):
   ```
   cd publisher
   cargo build --release
   cargo run
   ```

3. In another terminal, build and run the subscriber (display):
   ```
   cd subscriber
   cargo build --release
   cargo run
   ```

## How It Works

### Publisher (Temperature/Humidity Sensor)

The publisher application:
1. Connects to the MQTT broker
2. Generates random temperature and humidity values
3. Publishes JSON data to the topic: `home/room_sensor_livingroom/temperature_humidity`
4. Repeats this process every 5 seconds

Data format:
```json
{"temperature": 23.45, "humidity": 52.67}
```

### Subscriber (Display)

The subscriber application:
1. Connects to the MQTT broker
2. Subscribes to the topic: `home/room_sensor_livingroom/temperature_humidity`
3. Receives and displays incoming sensor data
4. Maintains connection with periodic ping messages

## MQTT Implementation Details

Both applications implement a subset of the MQTT protocol:

- **Connect**: Establishes connection with the broker
- **Publish**: Sends data to specific topics (publisher)
- **Subscribe**: Registers interest in specific topics (subscriber)
- **Ping**: Keeps connection alive (subscriber)

The implementation includes proper packet encoding/decoding and connection management.
