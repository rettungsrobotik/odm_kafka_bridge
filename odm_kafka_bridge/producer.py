#!/usr/bin/env python

from confluent_kafka import Producer
import json


def produce(data: dict, topic: str, key: str):
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    print("Sending message...")
    producer.produce(
        topic=topic,
        key=key,
        value=json.dumps(data),
        callback=callback,
    )
    producer.flush()


def callback(err, msg):
    if not err:
        print(f"Message succcessfully sent to {msg.topic()}")
    else:
        print(f"Error occured: {err}")


if __name__ == "__main__":
    test_data = {"msg": "Hello, world!"}
    produce(test_data, "example-topic", "test-key")
