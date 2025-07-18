#!/usr/bin/env python

"""
Unit tests for odm_kafka_bridge.producer
"""

from odm_kafka_bridge.producer import produce
from .mock_consumer import KafkaMockConsumer

import time


def test_produce_message(kafka_mock_consumer):
    """
    Checks if arbitary data can be produced and consumed via the local test cluster.

    Parameters:
    - kafka_mock_consumer: fixture provided by conftest.py
    """
    consumer: KafkaMockConsumer = kafka_mock_consumer
    assert consumer is not None, "Fixture is not injected properly"

    test_data = {
        "message": "Hello from producer",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    topic = "example-topic"
    key = "test-key"

    produce(test_data, topic=topic, key=key)
    time.sleep(1)  # allow time for async delivery and consumption

    received = consumer.get_last_message(key)
    assert received == test_data
