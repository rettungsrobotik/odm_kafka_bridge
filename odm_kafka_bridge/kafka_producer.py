#!/usr/bin/env python

from confluent_kafka import Producer
import json


def produce(data: dict, topic: str, key: str) -> None:
    """
    Sends a message to a Kafka topic.

    Args:
        data: a dict of the data to send.
        topic: name of the Kakfa topic to send the data to.
        key: key of the message (used by Kafka to assign partition, ensure ordering).
    """

    producer = Producer({"bootstrap.servers": "localhost:9092"})

    print("Sending message...")
    producer.produce(
        topic=topic,
        key=key,
        value=json.dumps(data),
        callback=callback,
    )
    producer.flush()


def callback(err, msg) -> None:
    """
    Gets called by Kafka after a message as been sent.

    Args:
        err: error message
        msg: normal message
    """

    if not err:
        print(f"Message succcessfully sent to {msg.topic()}")
    else:
        print(f"Error occured: {err}")


if __name__ == "__main__":
    test_data = {"msg": "Hello, world!"}
    produce(test_data, "example-topic", "test-key")
