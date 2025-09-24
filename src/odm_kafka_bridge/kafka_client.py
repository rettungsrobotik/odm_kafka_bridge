#!/usr/bin/env python

from confluent_kafka import Consumer, KafkaError, Message, Producer, TopicPartition
from io import BytesIO
import logging
from pathlib import Path
from time import time
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

        Raises:
            AssertionError
            RuntimeError
        """

        self.log = logging.getLogger("kafka")
        log_lvl = logging.DEBUG if debug else logging.INFO
        self.log.setLevel(log_lvl)

        self.url = config["kafka"]["url"]
        self.log.debug(f"Connecting to Kafka @ {self.url}")

        # configuration shared by producer & consumer
        conf_common = {
            "bootstrap.servers": self.url,
            "message.max.bytes": 104857600,  # 100 MiB
            "socket.timeout.ms": 30000,  # 30sec
        }
        # authentification config
        kafka_auth = config["kafka"].get("auth", None)
        if kafka_auth is not None:
            assert username is not None and username != "", "Kafka username required!"
            assert password is not None and password != "", "Kafka password required!"
            assert (
                ssl_pwd is not None and ssl_pwd != ""
            ), "Kafka SSL key password required!"

            # Locate certificates
            cert_dir = Path(kafka_auth["cert_dir"]).resolve()
            self.log.info(f"Loading SSL keys and certificates from {cert_dir}")
            if not cert_dir.exists() or not cert_dir.is_dir():
                raise RuntimeError(f"{cert_dir} does not exist or is not a directory!")

            path_ca = cert_dir / Path(kafka_auth["ca"])
            path_crt = cert_dir / Path(kafka_auth["client_crt"])
            path_key = cert_dir / Path(kafka_auth["client_key"])
            self.log.debug(f"Loading CA certificate from: {path_ca}")
            self.log.debug(f"Loading client certificate from: {path_crt}")
            self.log.debug(f"Loading client key from: {path_key}")
            for p in [path_ca, path_crt, path_key]:
                assert p.exists(), f"Kafka SSL key or certificate missing"

            # Append auth info to producer configuration
            conf_common["security.protocol"] = "SASL_SSL"
            conf_common["sasl.mechanism"] = "PLAIN"
            conf_common["sasl.username"] = username
            conf_common["sasl.password"] = password
            conf_common["ssl.ca.location"] = str(path_ca)
            conf_common["ssl.certificate.location"] = str(path_crt)
            conf_common["ssl.key.location"] = str(path_key)
            conf_common["ssl.key.password"] = ssl_pwd
        else:
            self.log.warning("No authentification for Kafka cluster configured")

        # create producer
        conf_producer = {
            "message.timeout.ms": 30000,
            "batch.num.messages": 1,
            "linger.ms": 0,
        }
        self.producer = Producer({**conf_common, **conf_producer})

        # create consumer
        conf_consumer = {
            "group.id": "odm-kafka-bridge",
            "auto.offset.reset": "latest",
            "enable.partition.eof": True,  # can be used to detect end of stream
        }
        self.consumer = Consumer({**conf_common, **conf_consumer})
        self.topic_partition = TopicPartition(config["kafka"]["topic"], 0)
        self.consumer.assign([self.topic_partition])
        self.consumer.poll(0.1)  # ensure assignment is processed

        return

    def close(self) -> None:
        """
        Cleanup of Kafka interfaces.
        """
        self.producer.flush()
        self.consumer.close()
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

        self.log.info(f"Connected to {self.url}")
        return

    def has_been_produced_already(
        self,
        task_id: str,
        asset_name: str,
        topic: str,
        key: str,
        timeout: float = 10.0,
        num_last_messages: int = 3,
    ) -> bool:
        """
        Returns True if the task ID appears in the headers of any of the last N messages.

        Args:
            task_id: Task ID to check.
            topic: Kafka topic.
            key: Message key to filter on.
            timeout: Max time to wait for messages.
            num_last_messages: Number of trailing messages to inspect.
        """
        assert timeout > 0.0 and num_last_messages >= 1
        self.log.info(
            f"Checking for previous messages in topic '{topic}' ({timeout=}s)"
        )

        # Can't use consumer.subscribe(): if group.id is the same, the consumer won't
        # get messages that a consumer in a previous process has already received.
        # Instead, use partition offsets to reliably get last N messages every time.
        low, high = self.consumer.get_watermark_offsets(self.topic_partition)
        offset = max(low, high - num_last_messages)
        self.consumer.seek(TopicPartition(topic, 0, offset))
        self.log.debug(f"Starting from offset {offset}")

        start = time()
        while (time() - start) < timeout:
            msg = self.consumer.poll(0.5)
            if not msg:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    self.log.warning(f"Kafka error: {msg.error()}")
                continue

            if msg.key() != key.encode("utf-8"):
                continue

            headers = dict(msg.headers() or [])
            msg_task_id = headers.get("task_id", b"").decode("utf-8")
            msg_asset_name = headers.get("asset_name", b"").decode("utf-8")
            if msg_task_id == task_id and msg_asset_name == asset_name:
                self.log.debug(f"Duplicate message found at offset {msg.offset()}")
                self.consumer.unassign()
                return True

        self.log.info("No duplicate message found before timeout.")
        self.consumer.unassign()
        return False

    def produce(
        self, asset: BytesIO, headers: list[tuple], topic: str, key: str
    ) -> None:
        """
        Sends a message to a Kafka topic.

        Args:
            asset: Raw asset bytes to send.
            headers: Structured metadata, e.g. project name and task ID.
            topic: name of the Kakfa topic to send the data to.
            key: key of the message (used by Kafka to assign partition, ensure ordering).
        """

        self.log.debug(f"Producing message to {topic=} with {key=}")
        self.producer.produce(
            topic=topic,
            key=key,
            value=asset.read(),
            headers=headers,
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

        Raises:
            RuntimeError
        """
        if err:
            self.log.error("An error occured while sending the message to Kafka!")
            raise RuntimeError(f"{err}")
        else:
            self.log.info(f"Message sent successfully to Kafka topic '{msg.topic()}'")
            self.log.debug(f"in partition {msg.partition()} at offset {msg.offset()}")

        return
