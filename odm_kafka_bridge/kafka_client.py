#!/usr/bin/env python

from confluent_kafka import Message, Producer
import json


class KafkaClient:
    """Interacts with a Kafka cluster."""

    def __init__(self, url: str, debug: bool = False):
        """
        Constructor.

        Args:
            url: Kafka URL, e.g. localhost:9092
            debug: Enable debug output
        """

        self.producer = Producer(
            {
                "bootstrap.servers": url,
                "message.max.bytes": 104857600,  # 100 MiB
            }
        )
        self._debug = debug

    def produce(self, data: dict, topic: str, key: str) -> None:
        """
        Sends a message to a Kafka topic.

        Args:
            data: a dict of the data to send.
            topic: name of the Kakfa topic to send the data to.
            key: key of the message (used by Kafka to assign partition, ensure ordering).
        """

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
            print(f"[DEBUG] [KafkaClient] {msg}")


if __name__ == "__main__":
    client = KafkaClient("localhost:9092")
    test_data = {"msg": "Hello, world!"}
    client.produce(test_data, "example-topic", "test-key")
