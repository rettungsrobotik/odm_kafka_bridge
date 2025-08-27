#!/usr/bin/env python

from odm_client import ODMClient
from kafka_client import KafkaClient

from base64 import b64encode
import logging
from pathlib import Path
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

    log = logging.getLogger("bridge")

    # Prepare WebODM client
    odm_url = config["webodm"]["url"]
    project_name = config["webodm"]["project"]
    asset_name = config["webodm"].get("asset", "dsm.tif")
    odm = ODMClient(
        base_url=odm_url, username=odm_user, password=odm_password, debug=debug
    )
    odm.authenticate()

    # Prepare Kafka client
    kafka = KafkaClient(
        config,
        kafka_user,
        kafka_password,
        kafka_ssl_key_pw,
        debug=debug,
    )
    kafka.verify_connection()

    # Find project and task with asset
    project_id = odm.get_project_id_by_name(project_name)

    task_id = odm.get_latest_task_with_asset(project_id, asset_name=asset_name)
    if not task_id:
        raise RuntimeError(f"No task in {project_id=} with asset {asset_name=} found!")

    # Download
    tmp_file = odm.download_asset(project_id, task_id, asset_name)
    try:
        # serialize
        tmp_file.seek(0)
        binary_data: bytes = tmp_file.read()
        serialized_data = b64encode(binary_data).decode("utf-8")
    finally:
        tmp_file.close()

    message = {
        "project_name": project_name,
        "project_id": project_id,
        "task_id": task_id,
        "asset_name": asset_name,
        "asset": serialized_data,
    }
    if debug:
        from humanfriendly import format_size

        bin_size = format_size(getsizeof(binary_data))
        log.debug(f"Size of binary data: {bin_size}")
        sd_size = format_size(getsizeof(serialized_data))
        log.debug(f"Size of serialized data: {sd_size}")

    topic = config["kafka"]["topic"]
    key = config["kafka"]["key"]
    kafka.produce(message, topic=topic, key=key)

    return
