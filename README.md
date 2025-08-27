# CREXDATA WebODM→Kafka Bridge

This project implements a bridge between a WebODM instance and an Apache Kafka cluster. 
It finds and downloads assets from a given ODM project, such as "Digital Surface Models" (DSMs), and pushes them to a given Kafka topic.

Workflow:
1. Load config.toml and SSL certificates.
2. Authenticate with WebODM and Kafka servers.
3. Search for a project by the configured name.
4. Get the newest task within this project that has the desired asset (e.g., `dsm.tif`).
5. Download asset and push it to Kafka.

The Kafka messages are JSON-formatted:
```javascript
message = {
        "project_name": str,
        "project_id": int,
        "task_id": uuid,
        "asset_name": str,
        "asset": bytes,
}
```

## Getting Started

### 1. Install dependencies

Python >= 3.8 required.

```bash
pip install -r requirements.txt
```

### 2. Prepare authentication

The keys provided by the CREXDATA server admins (`kafka.truststore.jks` and `kafka.keystore.jks`) must be converted to `.key` and `.crt` files to be compatible with `confluent_kafka`.
Put these files into the [odm_kafka_bridge/config](odm_kafka_bridge/config) folder.

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

### 4. Run

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

Note: 

