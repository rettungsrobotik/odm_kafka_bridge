#!/usr/bin/env python

from odm_client import ODMClient
from kafka_client import KafkaClient

from humanfriendly import format_size
from io import BytesIO
import logging
from sys import getsizeof
from typing import Optional


def run_bridge(
    config: dict,
    odm_user: str,
    odm_password: str,
    kafka_user: Optional[str],
    kafka_password: Optional[str],
    kafka_ssl_key_pw: Optional[str],
    debug: bool = False,
) -> None:
    """
    Download from WebODM and upload to Kafka.

    Args:
        config: dict loaded from toml file
        odm_user: WebODM username.
        odm_password: WebODM password.
        kafka_user: optional Kafka SASL username.
        kafka_password: optional Kafka SASL password.
        kafka_ssl_key_pw: optional Key to Kafka client SSL certificate.
        debug: Enable debug output.

    Raises:
        RuntimeError
    """

    log_lvl = logging.DEBUG if debug else logging.INFO
    log = logging.getLogger("bridge")
    log.setLevel(log_lvl)

    # Prepare WebODM client
    odm_url = config["webodm"]["url"]
    project_name = config["webodm"]["project"]
    asset_name = config["webodm"].get("asset", "dsm.tif")
    odm = ODMClient(odm_url, odm_user, odm_password, debug)
    odm.authenticate()

    # Prepare Kafka client
    kafka = KafkaClient(config, kafka_user, kafka_password, kafka_ssl_key_pw, debug)
    kafka.verify_connection()

    # Find project and task with asset
    project_id = odm.get_project_id_by_name(project_name)
    task_id = odm.get_latest_task_with_asset(project_id, asset_name)
    if not task_id:
        raise RuntimeError(f"No task in {project_id=} with asset {asset_name=} found!")

    # Download
    asset: BytesIO = odm.download_asset(project_id, task_id, asset_name)
    log.debug(f"Downloaded {format_size(getsizeof(asset))}")

    # Upload
    headers = [
        ("project_name", str(project_name)),
        ("project_id", str(project_id)),
        ("task_id", str(task_id)),
        ("asset_name", str(asset_name)),
    ]
    topic = config["kafka"]["topic"]
    key = config["kafka"]["key"]
    kafka.produce(asset, headers, topic, key)

    return
