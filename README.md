# ODM Kafka Bridge (for CREXDATA)

This project implements a bridge that downloads orthophotos and DSMs from WebODM via its REST API,
and pushes them to Apache Kafka topics.

## Features

- Designed to work with existing WebODM data structures (JSON format)
- Local test environment with Docker Compose

## Requirements

- Base: Python 3.8, `confluent-kafka` library
- For local testing: pytest, docker + docker compose

## Getting Started

### 1. Install dependencies

```bash
pip install -r requirements.txt
```
### 2. Prepare environment

1. Copy [example.env](odm_kafka_bridge/example.env) to `.env` and fill in your credentials for WebODM and Kafka.
2. Edit [config.toml)(odm_kafka_bridge/config.toml) to set server URLs and other configuration parameters.

### 3. Run

The core logic in [odm_kafka_bridge](odm_kafka_bridge) can be imported into other Python modules.

A commandline interface is provided as well. Check the available options with:

```bash
odm_kafka_bridge.py -h
```

