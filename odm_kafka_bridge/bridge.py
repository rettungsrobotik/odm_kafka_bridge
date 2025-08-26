#!/usr/bin/env python

from odm_client import ODMClient
from kafka_client import KafkaClient

from base64 import b64encode
from sys import getsizeof
from tempfile import TemporaryFile


def run_bridge(
    odm_url: str,
    odm_user: str,
    odm_password: str,
    project_name: str,
    asset_name: str,
    kafka_url: str,
    kafka_topic: str,
    kafka_key: str,
    debug: bool = False,
) -> None:
    """
    Download an asset from WebODM and send it to Kafka.

    Args:
        odm_url: Base URL of WebODM.
        odm_user: WebODM username.
        odm_password: WebODM password.
        project_name: Name of the project in WebODM.
        asset_name: Name of the asset to download.
        kafka_url: URL of the Kafka cluster.
        kafka_topic: Kafka topic to publish to.
        kafka_key: Kafka message key.
        debug (bool): Enable debug output.

    Raises:
        RuntimeError
    """

    def _print_dbg(msg: str) -> None:
        """Helper function to print debug messages if flag ist set.

        Args:
            str: the debug message to print.
        """
        if debug:
            print(f"[DEBUG] {msg}")

    try:

        # ODM authentication
        odm = ODMClient(
            base_url=odm_url, username=odm_user, password=odm_password, debug=debug
        )
        _print_dbg(f"Trying to authenticate @ {odm_url} with user {odm_user}")
        odm.authenticate()
        _print_dbg("Authentication successful!")

        _print_dbg(f"Initializing Kafka producer @ {kafka_url}")
        kafka = KafkaClient(kafka_url)

        # asset identification
        _print_dbg(f"Fetching project ID for {project_name}")
        project_id = odm.get_project_id_by_name(project_name)
        _print_dbg(f"Got project ID f{project_id}")

        _print_dbg(f"Fetching tasks for {project_id} that have {asset_name=}")
        task_id = odm.get_latest_task_with_asset(project_id, asset_name=asset_name)
        if not task_id:
            raise RuntimeError(
                f"No task in {project_id=} with asset {asset_name=} found!"
            )
        _print_dbg(f"Found task {task_id}")

        # download
        _print_dbg(f"Downloading {asset_name}")
        tmp_file = odm.download_asset(project_id, task_id, asset_name)

        # upload
        tmp_file.seek(0)
        binary_data: bytes = tmp_file.read()
        message = {
            "project": project_name,
            "task_id": task_id,
            "asset_name": asset_name,
            # "asset_b64": b64encode(binary_data),
        }
        if debug:
            from humanfriendly import format_size

            bin_size = format_size(getsizeof(binary_data))
            _print_dbg(f"Size of binary data: {bin_size}")
            msg_size = format_size(getsizeof(message))
            _print_dbg(f"Size of encoded message: {msg_size}")

        _print_dbg(f"Producing message to {kafka_topic=} with {kafka_key=}")
        kafka.produce(message, topic=kafka_topic, key=kafka_key)

    except Exception as e:
        print(f"An error occured: {e}")
        # drop into debugger automatically
        if debug:
            import pdb

            pdb.post_mortem()

    _print_dbg(f"run_bridge() finished successfully")
    return
