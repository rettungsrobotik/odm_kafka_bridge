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

Convert provided truststore and keystore to confluent_kafka compatible key and crt files:

```bash
# Export CA certificate from truststore
keytool -importkeystore -srckeystore kafka.truststore.jks \
        -srcstoretype JKS \
        -destkeystore truststore.p12 \
        -deststoretype PKCS12 \
        -srcstorepass <truststore-password>

openssl pkcs12 -in truststore.p12 -nokeys -out config/kafka_ca.crt

# Export client cert & key from keystore
keytool -importkeystore -srckeystore kafka.keystore.jks \
        -srcstoretype JKS \
        -destkeystore keystore.p12 \
        -deststoretype PKCS12 \
        -srcstorepass <keystore-password>

openssl pkcs12 -in keystore.p12 -nocerts -nodes -out config/kafka_client.key
openssl pkcs12 -in keystore.p12 -nokeys -out config/kafka_client.crt
```

Copy [example.env](odm_kafka_bridge/config/example.env) to `config/.env` and fill in your credentials for WebODM, Kafka, and Kafka client SSL certificate.

Edit [config.toml](odm_kafka_bridge/config/config.toml) to set server URLs and other configuration parameters.

### 3. Run

The core logic in [odm_kafka_bridge](odm_kafka_bridge) can be imported into other Python modules.

A commandline interface (CLI) is provided as well. Check the available options with:

```bash
cli.py -h
```

