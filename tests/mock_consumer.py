#!/usr/bin/env python

from confluent_kafka import Consumer
import json

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "mock-consumer",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(conf)
consumer.subscribe(["example-topic"])

print("MockConsumer waiting for messages...")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"{msg.error()}")
        else:
            print(f"Received message: {json.loads(msg.value().decode())}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()

print("MockConsumer terminates")
