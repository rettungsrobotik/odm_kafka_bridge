#!/usr/bin/env python

from confluent_kafka import Message, Producer
import json
from pathlib import Path
from typing import Optional


class KafkaClient:
    """Interacts with a Kafka cluster."""

    def __init__(
        self,
        config: dict,
        username: str,
        password: str,
        ssl_pwd: str = "",
        debug: bool = False,
    ):
        """
        Constructor.

        Args:
            config: dict with configuration
            username: SASL username
            password: SASL password
            ssl_pwd: SSL certificate password
            debug: Enable debug output
        """

        self._debug = debug

        url = config["kafka"]["url"]
        ssl = config["kafka"]["ssl"]
        self._print_dbg(f"Initializing Kafka producer @ {url}")
        self._print_dbg(f"SSL config: {ssl}")
        module_dir = Path(__file__).parent
        self.producer = Producer(
            {
                "bootstrap.servers": config["kafka"]["url"],
                "message.max.bytes": 104857600,  # 100 MiB
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": username,
                "sasl.password": password,
                "ssl.ca.location": module_dir / ssl["ca_location"],
                "ssl.certificate.location": module_dir / ssl["certificate_location"],
                "ssl.key.location": module_dir / ssl["key_location"],
                "ssl.key.password": ssl_pwd,
                "socket.timeout.ms": 60000,  # 1 min
                "message.timeout.ms": 60000,
                "linger.ms": 0,
                "batch.num.messages": 1,
            }
        )

    def verify_connection(
        self, topic: Optional[str] = None, timeout: float = 5.0
    ) -> None:
        """
        Tries to fetch cluster metadata to verify the connection.

        Args:
            topic (str): Optional topic name to check existence of
            timeout (float): Timeout in seconds

        Raises:
            RuntimeError: If metadata cannot be retrieved
        """
        self._print_dbg("Verifying connection")
        try:
            md = self.producer.list_topics(timeout=timeout)
            if topic and topic not in md.topics:
                raise RuntimeError(f"Topic '{topic}' not found in cluster")
        except Exception as e:
            raise RuntimeError(f"Kafka connection verification failed: {e}")

    def produce(self, data: dict, topic: str, key: str) -> None:
        """
        Sends a message to a Kafka topic.

        Args:
            data: a dict of the data to send.
            topic: name of the Kakfa topic to send the data to.
            key: key of the message (used by Kafka to assign partition, ensure ordering).
        """

        self._print_dbg(f"Producing message to {topic=} with {key=}")
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
            print(f"[ERROR] {err}")
        else:
            print(f"[INFO] Message sent successfully to {msg.topic()}")

        return

    def _print_dbg(self, msg: str):
        if self._debug:
            print(f"[DEBUG] [Kafka] {msg}")
