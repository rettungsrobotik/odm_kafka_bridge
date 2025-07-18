#!/usr/bin/env python

"""
Defines fixtures to be auto-loaded by pytest.
"""

from .local_kafka import LocalKafkaStack
from .mock_consumer import KafkaMockConsumer

import pytest


@pytest.fixture(scope="session", autouse=False)
def kafka_stack():
    kafka = LocalKafkaStack()
    kafka.start()
    yield
    kafka.stop()


@pytest.fixture(scope="function")
def kafka_mock_consumer(kafka_stack):
    consumer = KafkaMockConsumer(topic="example-topic")
    consumer.start()
    consumer.wait_until_ready()
    yield consumer
    consumer.stop()
