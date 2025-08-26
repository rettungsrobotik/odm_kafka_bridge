#!/usr/bin/env python

from confluent_kafka import Message, Producer
import json


class KafkaProducer:
    """Pushes data to a Kafka topic."""

    def __init__(self, url: str):
        """
        Constructor.

        Args:
            url: Kafka URL, e.g. localhost:9092
        """

        self.producer = Producer({"bootstrap.servers": url})

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


if __name__ == "__main__":
    producer = KafkaProducer("localhost:9092")
    test_data = {"msg": "Hello, world!"}
    producer.produce(test_data, "example-topic", "test-key")
