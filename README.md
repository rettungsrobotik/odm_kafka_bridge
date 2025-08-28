# CREXDATA ODM→Kafka Bridge

This Python project finds and downloads assets from an Open Drone Map (ODM) project,
such as "Digital Surface Models" (DSMs), and pushes them to an Apache Kafka topic.

*Workflow*
1. Load configuration from `config.toml` and credentials from `.env`. If the config has a kafka.auth block, also load SSL certificates.
2. Connect and authenticate to WebODM and Kafka servers.
3. Determine WebODM project id of the configured project name.
4. Get the newest task within the project that has the desired asset (e.g., `dsm.tif`).
5. Download asset and push it to Kafka.
6. Wait bridge.monitor_interval_sec (if set) and repeat steps 4+5 until the program is terminated.

*Message format*

The asset will be produced to Kafka in raw byte form.
The *headers* will contain:
* `project_name`
* `project_id`
* `task_id`
* `asset_name`

## Getting Started

### 1. Install

Python >= 3.10 required.

Install the dependencies (see [pyproject.toml](pyproject.toml)) and the program itself with:

```bash
cd <project root>
pip install .
```

### 2. Prepare authentication

#### Convert provided Kafka keys and certificates

The `.jks` files provided by the CREXDATA server admins (`kafka.truststore.jks` and `kafka.keystore.jks`)
must be converted to `.key` and `.crt` files to be compatible with `confluent_kafka`.

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

#### Put credentials into dotenv

Copy [config/example.env](config/example.env) to `config/.env` and fill in the required username and password fields.

### 3. Run

Edit [config/config.toml](config/config.toml) to set server URLs, ODM project name, Kafka topic name, certificate paths etc.

Then, run the command line interface (CLI):
```bash
odm-kafka-bridge -c <path-to-config-dir>
```

Check available options (e.g., debug mode) with:
```bash
odm-kafka-bridge --help
```

Example console output:
```
[2025-08-28 15:01:26,051] [INFO] [cli] Loading configuration from config/config.toml
[2025-08-28 15:01:26,052] [INFO] [cli] Loading credentials from config/.env
[2025-08-28 15:01:26,052] [INFO] [kafka] No authentication configured
[2025-08-28 15:01:26,109] [INFO] [kafka] Connected and authenticated @ localhost:9092
[2025-08-28 15:01:26,303] [INFO] [odm] Connected and authenticated @ https://webodm.rettungsrobotik.de
[2025-08-28 15:01:26,303] [INFO] [bridge] Starting ODM->Kafka bridge
[2025-08-28 15:01:26,304] [INFO] [bridge] Monitoring for fresh assets every 60s
[2025-08-28 15:01:26,462] [INFO] [odm] Found ID '5' for project name 'CREXDATA Trials'
[2025-08-28 15:01:26,462] [INFO] [bridge] Checking for new tasks with asset 'dsm.tif'
[2025-08-28 15:01:26,607] [INFO] [bridge] Found task a7cdd3c7-7a9a-4f1e-abc1-7853eff50c65
[2025-08-28 15:01:26,607] [INFO] [bridge] Downloading asset dsm.tif...
[2025-08-28 15:01:30,254] [INFO] [bridge] Downloaded 26.43 MiB
[2025-08-28 15:01:30,336] [INFO] [kafka] Message sent successfully to Kafka topic UPB-CREXDATA-RealtimeDEM-Stream
```

## Development

The core module in [src/odm_kafka_bridge](src/odm_kafka_bridge) can be imported by other projects:

```python
from odm_kafka_bridge.bridge import run_bridge
```

A local Kafka cluster for testing can be spun up using docker compose.
Set `kafka.url` in `config.toml` to `localhost:9092` and remove the `kafka.auth` block since it does not require authentication.

```bash
docker compose -f test/docker-compose.yml up
```

### Possible extensions and improvements

* Include timestamps in Kafka message headers
* Relay multiple assets in one go (e.g., DSM and orthophoto and 3D model).
* Two-way bridge — receive drone survey images from Kafka and use them to create a new WebODM task.
* Systemd wrapper
* Unit tests
