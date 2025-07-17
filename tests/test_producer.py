#!/usr/bin/env python

"""
Unit tests for odm_kafka_bridge.producer
"""

from odm_kafka_bridge.producer import produce

import time


def test_produce_message():
    # start_kafka()

    test_data = {
        "message": "Hello from test_producer.py",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    produce(test_data, topic="example-topic", key="test-key")
