#!/usr/bin/env python

import threading
import time
import json
from confluent_kafka import Consumer
from collections import defaultdict


class KafkaMockConsumer:
    """
    Receives messages from a Kafka cluster on a given topic and key.
    Stores received messages to enable checking if sent=received.
    """

    def __init__(
        self,
        topic="example-topic",
        group_id="test-consumer",
        bootstrap_servers="localhost:9092",
    ):
        """Constructor"""

        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self._thread_running = threading.Event()
        self._consume_loop_running = threading.Event()
        self._partition_assigned = threading.Event()
        self.thread = None
        self.messages = defaultdict(dict)  # topic -> key -> last message
        print(f"MockConsumer initialized for topic {topic}")

    def wait_until_ready(self, timeout=20):
        """
        Blocks until the consume loop is running.

        Parameters:
        - timeout: Max. time to wait in seconds

        Raises:
        - RuntimeError if loop was not started before timeout.
        """

        self._consume_loop_running.wait(timeout=timeout)
        if not self._consume_loop_running:
            raise RuntimeError("consume loop not entered before timeout")

    def on_assign(self, c, partitions):
        """
        Callback to run when the consumer was assigned partitions.
        """

        self._partition_assigned.set()
        print("MockConsumer: partitions assigned")

    def _consume_loop(self) -> None:

        conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
        }

        consumer = Consumer(conf)
        consumer.subscribe([self.topic], on_assign=self.on_assign)
        print("MockConsumer subscribing")
        while not self._thread_running.is_set():
            time.sleep(0.2)  # spin until start() sets running event

        self._partition_assigned.wait(timeout=10)

        print(f"MockConsumer entering loop")
        while self._thread_running.is_set():

            self._consume_loop_running.set()

            msg = consumer.poll(0.5)
            if msg is None or msg.error():
                continue
            elif msg.error():
                print(f"Error receiving message!")
            else:
                try:
                    key = msg.key().decode() if msg.key() else None
                    value = json.loads(msg.value().decode())
                    self.messages[key] = value
                    print(f"MockConsumer received message: {key=}, {value=}")
                except Exception as e:
                    print(f"Error decoding message: {e}")

        self._consume_loop_running.clear()
        self._partition_assigned.clear()
        consumer.close()

    def start(self) -> None:
        """Starts the consume loop."""

        print("MockConsumer starting loop")
        self._thread_running.set()
        self.thread = threading.Thread(target=self._consume_loop, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        """Stops the consume loop."""

        self._thread_running.clear()
        print("MockConsumer stoppping loop")
        if self.thread:
            self.thread.join()

    def get_last_message(self, key) -> dict:
        """
        Fetches a received message with given key from storage.

        Parameters:
        - key: Key of the message

        Returns:
        - dict containing the message
        """
        return self.messages[key]
