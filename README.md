# CREXDATA ODM→Kafka Bridge

- [Introduction](#introduction)
   * [Workflow](#workflow)
   * [Message format](#message-format)
- [Getting Started](#getting-started)
   * [1. Install](#1-install)
   * [2. Prepare authentication](#2-prepare-authentication)
      + [Convert Kafka keys and certificates](#convert-kafka-keys-and-certificates)
      + [Store credentials](#store-credentials)
   * [3. Run](#3-run)
- [Development](#development)
   * [Local testing](#local-testing)
   * [Possible extensions and improvements](#possible-extensions-and-improvements)

## Introduction

This Python project, developed in the context of the [CREXDATA](https://crexdata.eu) project,
serves as a bridge between the DRZ' [Open Drone Map (ODM)](https://opendronemap.org) instance and the CREXDATA system,
which is build on [Apache Kafka](https://kafka.apache.org/).
It can download assets, such as Digital Surface Models (DSMs) from ODM and relay them to a Kafka topic.

### Workflow

The tool...

1. Loads configuration from `config.toml` and credentials from `.env`.
If the config has a kafka.auth block, it also loads SSL certificates.
2. Connects to WebODM and Kafka servers.
3. Determines WebODM project ID of the configured project name.
4. Gets the newest task within the project that has the desired asset (e.g., `dsm.tif`).
5. Checks if the asset has already been produced to Kafka.
6. If not: downloads the asset from WebODM, wraps it with some headers and produces it to a Kafka topic.

By default, stepts 4–6 are repated with 60 seconds waiting time between iterations.
The tool can also run in "oneshot" mode, by configuring the time between iterations to `-1`.

### Message format

The asset will be produced to Kafka as a raw-byte stream. The headers contain:

- `project_name`
- `project_id`
- `task_id`
- `asset_name`

---

## Getting Started

### 1. Install

Python >= 3.10 required.
Install the dependencies (see [pyproject.toml](pyproject.toml)) and the program itself with:

```bash
cd <project root>
pip install .
```

### 2. Prepare authentication

#### Convert Kafka keys and certificates

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
mkdir config/certs
openssl pkcs12 -in truststore.p12 -nokeys -out config/certs/kafka_ca.crt
openssl pkcs12 -in keystore.p12 -nocerts -nodes -out config/certs/kafka_client.key
openssl pkcs12 -in keystore.p12 -nokeys -out config/certs/kafka_client.crt
```

#### Store credentials

Copy [config/example.env](config/example.env) to `config/.env`
and fill in the required username and password fields.
The Kafka credentials may be omitted if the server does not require authentication.

### 3. Run

Edit [config/config.toml](config/config.toml) to set
server URLs, ODM project name, Kafka topic name, certificate paths etc.

Then, run the command line interface (CLI):

```bash
odm-kafka-bridge -c <path-to-config-dir>
```

Check available options (e.g., debug mode) with:

```bash
odm-kafka-bridge --help
```

Example console output:

```text
[2025-09-03 12:02:04,135] [INFO] [cli] Loading configuration from config/config.toml
[2025-09-03 12:02:04,135] [INFO] [cli] Loading credentials from config/.env
[2025-09-03 12:02:04,136] [INFO] [kafka] Loading SSL keys and certificates from /home/merlin/work/odm_kafka_bridge/config/certs
[2025-09-03 12:02:04,260] [INFO] [kafka] Connected to server.crexdata.eu:9092
[2025-09-03 12:02:04,482] [INFO] [odm] Connected to https://webodm.rettungsrobotik.de
[2025-09-03 12:02:04,482] [INFO] [bridge] Starting ODM->Kafka bridge
[2025-09-03 12:02:04,482] [INFO] [bridge] Will check for fresh assets every 60s
[2025-09-03 12:02:04,672] [INFO] [odm] Found ID '5' for project name 'CREXDATA Trials'
[2025-09-03 12:02:04,673] [INFO] [odm] Getting latest task with asset 'dsm.tif'
[2025-09-03 12:02:04,832] [INFO] [odm] Found task 'a7cdd3c7-7a9a-4f1e-abc1-7853eff50c65'
[2025-09-03 12:02:04,832] [INFO] [odm] Downloading asset 'dsm.tif' ...
[2025-09-03 12:02:11,480] [INFO] [odm] Downloaded 26.43 MiB
[2025-09-03 12:02:13,358] [INFO] [kafka] Message sent successfully to Kafka topic 'UPB-CREXDATA-RealtimeDEM-Stream'
[2025-09-03 12:03:13,372] [INFO] [odm] Getting latest task with asset 'dsm.tif'
[2025-09-03 12:03:13,532] [INFO] [odm] Found task 'a7cdd3c7-7a9a-4f1e-abc1-7853eff50c65'
[2025-09-03 12:03:13,533] [INFO] [bridge] It's the same task as last time, going back to sleep
```

---

## Development

The core module in [src/odm_kafka_bridge](src/odm_kafka_bridge)
can be imported by other projects:

```python
from odm_kafka_bridge.bridge import run_bridge
```

### Local testing

A Kafka cluster for testing can be spun up on localhost using docker compose.
Set `kafka.url` in `config.toml` to `localhost:9092`
and remove the `kafka.auth` block, since it does not require authentication.

```bash
docker compose -f test/docker-compose.yml up
```

### Possible extensions and improvements

- Include timestamps in Kafka message headers
- Relay multiple assets in one go (e.g., DSM and orthophoto and 3D model).
- Two-way bridge: receive survey images from Kafka and create a new WebODM task.
- `systemd` wrapper
