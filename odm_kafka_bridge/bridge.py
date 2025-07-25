#!/usr/bin/env python

from odm_client import ODMClient
from kafka_producer import produce

from base64 import b64encode


def run_bridge(
    odm_url: str,
    odm_user: str,
    odm_password: str,
    project_name: str,
    asset_name: str,
    kafka_topic: str,
    kafka_key: str,
    debug: bool = False,
) -> None:
    """
    Download an asset from WebODM and send it to Kafka.

    Args:
        odm_url (str): Base URL of WebODM.
        odm_user (str): WebODM username.
        odm_password (str): WebODM password.
        project_name (str): Name of the project in WebODM.
        asset_name (str): Name of the asset to download.
        kafka_topic (str): Kafka topic to publish to.
        kafka_key (str): Kafka message key.
        debug (bool): Enable debug output.

    Raises:
        RuntimeError
    """

    # ODM authentication
    odm = ODMClient(base_url=odm_url, username=odm_user, password=odm_password)
    odm.authenticate()
    if debug:
        print("[DEBUG] Authenticated with WebODM")

    # asset identification
    project_id = odm.get_project_id_by_name(project_name)
    task_id = odm.get_latest_task_with_asset(project_id, asset_name=asset_name)
    if not task_id:
        raise RuntimeError(
            f"No task with asset '{asset_name}' found in project '{project_name}'"
        )
    if debug:
        print(f"[DEBUG] Found task {task_id} with asset '{asset_name}'")

    # asset download to temporary file
    temp_path = f"/tmp/{asset_name}"
    odm.download_asset(project_id, task_id, asset_name, temp_path)

    # push content to Kafka
    with open(temp_path, "rb") as f:
        binary_data: bytes = f.read()

        message = {
            "project": project_name,
            "task_id": task_id,
            "asset_name": asset_name,
            "asset_b64": b64encode(binary_data),
        }
        produce(message, topic=kafka_topic, key=kafka_key)
        if debug:
            print(
                f"[DEBUG] Asset pushed to Kafka topic '{kafka_topic}' with key '{kafka_key}'"
            )
