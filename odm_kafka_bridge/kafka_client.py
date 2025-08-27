#!/usr/bin/env python

from confluent_kafka import Message, Producer
import logging
import json
from pathlib import Path
from typing import Optional


class KafkaClient:
    """Interacts with a Kafka cluster."""

    def __init__(
        self,
        config: dict,
        username: Optional[str],
        password: Optional[str],
        ssl_pwd: Optional[str],
        debug: bool = False,
    ):
        """
        Constructor.

        Note: username, password and ssl_pwd are required if the config contains a
        kafka.auth block. If the block does not exist, it is assumed that the server does
        not require authentication.

        Args:
            config: dict with configuration
            cert_dir: optional path to certificate folder
            username: optional SASL username
            password: optional SASL password
            ssl_pwd: optional SSL certificate password
            debug: Enable debug output
        """

        self.log = logging.getLogger("kafka")
        log_lvl = logging.DEBUG if debug else logging.INFO
        self.log.setLevel(log_lvl)

        url = config["kafka"]["url"]
        producer_conf = {
            "bootstrap.servers": config["kafka"]["url"],
            "message.max.bytes": 104857600,  # 100 MiB
            "socket.timeout.ms": 30000,  # 30sec
            "message.timeout.ms": 30000,
            "linger.ms": 0,
            "batch.num.messages": 1,
        }

        kafka_auth = config["kafka"].get("auth", None)
        if kafka_auth is not None:
            assert username is not None and username != ""
            assert password is not None and password != ""
            assert ssl_pwd is not None and ssl_pwd != ""

            # Locate certificates
            cert_dir = Path(kafka_auth["cert_dir"])
            assert cert_dir.exists()
            path_ca = cert_dir / Path(kafka_auth["ca"])
            path_crt = cert_dir / Path(kafka_auth["client_crt"])
            path_key = cert_dir / Path(kafka_auth["client_key"])
            self.log.debug(f"Loading CA certificate from: {path_ca}")
            self.log.debug(f"Loading client certificate from: {path_crt}")
            self.log.debug(f"Loading client key from: {path_key}")
            for p in [path_ca, path_crt, path_key]:
                assert p.exists(), "Kafka SSL certificate missing"

            # Append auth info to producer configuration
            producer_conf["security.protocol"] = "SASL_SSL"
            producer_conf["sasl.mechanism"] = "PLAIN"
            producer_conf["sasl.username"] = username
            producer_conf["sasl.password"] = password
            producer_conf["ssl.ca.location"] = str(path_ca)
            producer_conf["ssl.certificate.location"] = str(path_crt)
            producer_conf["ssl.key.location"] = str(path_key)
            producer_conf["ssl.key.password"] = ssl_pwd
        else:
            self.log.info("No authentication configured")

        self.log.debug(f"Connecting to Kafka @ {url}")
        self.producer = Producer(producer_conf)
        return

    def verify_connection(
        self, topic: Optional[str] = None, timeout: float = 5.0
    ) -> None:
        """
        Tries to fetch cluster metadata to verify the connection.

        Args:
            topic: Optional topic name to check existence of
            timeout: Timeout [s]

        Raises:
            RuntimeError: If metadata cannot be retrieved.
        """
        self.log.debug("Verifying connection")
        try:
            md = self.producer.list_topics(timeout=timeout)
            if topic and topic not in md.topics:
                raise RuntimeError(f"Topic '{topic}' not found in cluster")

        except Exception as e:
            raise RuntimeError(f"Kafka connection verification failed: {e}")

        self.log.info(f"KafkaClient connected")

    def produce(self, data: dict, topic: str, key: str) -> None:
        """
        Sends a message to a Kafka topic.

        Args:
            data: a dict of the data to send.
            topic: name of the Kakfa topic to send the data to.
            key: key of the message (used by Kafka to assign partition, ensure ordering).
        """

        self.log.debug(f"Producing message to {topic=} with {key=}")
        self.producer.produce(
            topic=topic,
            key=key,
            value=json.dumps(data),
            callback=self.callback,
        )
        self.producer.flush()
        return

    def callback(self, err: str, msg: Message) -> None:
        """
        Gets called by Kafka after a message as been sent.

        Args:
            err: error message
            msg: normal message
        """
        if err:
            self.log.error(f"{err}")
        else:
            self.log.info(f"Message sent successfully to {msg.topic()}")

        return
