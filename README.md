# Smart Traffic Management System

## Overview
A real-time Smart Traffic Management System built using **Apache Kafka**, **Apache Spark Streaming**, **MongoDB**, and **Flask APIs** to simulate and process live traffic events, monitor congestion, optimize traffic signals, and prioritize emergency vehicles.

---

# Problem Statement
This project simulates a smart city traffic control system where traffic data is continuously generated, streamed through Kafka, processed in real-time using Spark Streaming, stored in MongoDB, and exposed through Flask REST APIs for analytics and monitoring.

The system supports:
- Real-time traffic event streaming
- Traffic congestion analysis
- Emergency vehicle prioritization
- Signal timing optimization
- Traffic analytics APIs
- Distributed stream processing architecture

---

# System Architecture

```text
Traffic Simulator (Kafka Producer)
                │
                ▼
        Apache Kafka Topic
         "traffic_events"
                │
                ▼
      Spark Streaming Processor
                │
        ┌───────┴────────┐
        ▼                ▼
 Traffic Analytics    Emergency Detection
        │                │
        └───────┬────────┘
                ▼
             MongoDB
                │
                ▼
            Flask APIs
                │
                ▼
     Dashboard / Postman / Frontend
```
---

# Tech Stack
| Technology | Purpose |
|---|---|
| Python | Core programming language |
| Apache Kafka | Real-time event streaming |
| Apache Spark Streaming | Stream processing |
| MongoDB | NoSQL database |
| Flask | REST API backend |
| Docker | Containerized services |

---

# Technologies Used
## Programming Languages
 * Python

 ## Frameworks & Libraries
* Flask
* PySpark
* Kafka-Python
* PyMongo

## Databases
 * MongoDB

# Project Structure

```text
smart-traffic-management-system/
│
├── backend/
│   ├── flask_api.py
│   ├── crud_operations.py
│   └── requirements.txt
│
├── streaming/
│   ├── kafka_producer.py
│   ├── spark_stream.py
│   └── test_spark.py
│
├── docker/
│   └── docker-compose.yaml
│
├── screenshots/
│
├── README.md
├── .gitignore
└── LICENSE
```
