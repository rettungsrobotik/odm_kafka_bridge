# ODM Kafka Bridge (for CREXDATA)

This project implements a bridge that downloads orthophotos and DSMs from WebODM via its REST API,
and pushes them to Apache Kafka topics.

## Features

- Designed to work with existing WebODM data structures (JSON format)
- Local test environment with Docker Compose

## Requirements

- Python 3.8+
- `confluent-kafka` library
- Docker + Docker Compose (for local Kafka cluster)

## Getting Started

### 1. Install dependencies

```bash
pip install -r requirements.txt
```
