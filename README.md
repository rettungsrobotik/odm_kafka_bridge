# CREXDATA WebODM<->Kafka Bridge

This project implements a bridge that downloads "Digital Surface Models" (DSMs) from WebODM via its REST API, and pushes them to Apache Kafka topics.

## Requirements


## Getting Started

### 1. Install dependencies

Python >= 3.8 required.

```bash
pip install -r requirements.txt
```

### 2. Prepare authentication certificates

The keys provided by the CREXDATA server admins (`kafka.truststore.jks` and `kafka.keystore.jks`) must be converted to `.key` and `.crt` files to be compatible with `confluent_kafka`.

```bash
# Export CA certificate from truststore to intermediate PKCS12 format
keytool -importkeystore -srckeystore kafka.truststore.jks \
        -srcstoretype JKS \
        -destkeystore truststore.p12 \
        -deststoretype PKCS12 \
        -srcstorepass <truststore-password>

# Export client cert & key from keystore as PKCS12
keytool -importkeystore -srckeystore kafka.keystore.jks \
        -srcstoretype JKS \
        -destkeystore keystore.p12 \
        -deststoretype PKCS12 \
        -srcstorepass <keystore-password>

# Convert from PKCS12
openssl pkcs12 -in truststore.p12 -nokeys -out config/kafka_ca.crt
openssl pkcs12 -in keystore.p12 -nocerts -nodes -out config/kafka_client.key
openssl pkcs12 -in keystore.p12 -nokeys -out config/kafka_client.crt
```

### 3. Edit configuration and prepare .env

* Edit [config.toml](odm_kafka_bridge/config/config.toml) to set server URLs, ODM project name, Kafka topic name, etc.
* Copy [example.env](odm_kafka_bridge/config/example.env) to `config/.env` and fill in your credentials for WebODM, Kafka, and Kafka client SSL certificate.

### 3. Run

This repository provides a commandline interface (CLI). Check the usage available options with:

```bash
cli.py --help
```

## Development

The core module in [odm_kafka_bridge](odm_kafka_bridge) can be imported by other projects:

```python
from odm_kafka_bridge import run_bridge
```

A local Kafka cluster for testing can be spun up using docker:

```bash
docker compose -f tests/docker-compose.yml up
```


