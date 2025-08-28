#!/usr/bin/env python

from odm_client import ODMClient
from kafka_client import KafkaClient

from humanfriendly import format_size
from io import BytesIO
import logging
from signal import signal, SIGINT, SIGTERM
from sys import getsizeof
from time import sleep
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
    Sets up ODM and Kafka clients, regularly searches for fresh assets (unless running
    in oneshot mode), produces the assets to a Kafka topic.

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

    # Prepare logger
    log_lvl = logging.DEBUG if debug else logging.INFO
    log = logging.getLogger("bridge")
    log.setLevel(log_lvl)
    log.info("Starting ODM->Kafka bridge")

    # Register signal handlers
    shutdown = False

    def handle_signal(signum, _):
        """
        Handles system signals for graceful shutdowns.

        Args:
            signum: Signal number
        """
        global shutdown
        shutdown = True
        logging.warning(f"Received signal {signum}, shutting down...")
        return

    signal(SIGINT, handle_signal)  # CTRL+C
    signal(SIGTERM, handle_signal)  # systemctl stop

    # Prepare sleep interval for monitor mode
    sleep_intrvl = int(config["bridge"].get("monitor_interval_sec", -1))
    if sleep_intrvl < 0:
        log.info("Running in oneshot mode")
    else:
        log.info(f"Monitoring for fresh assets every {sleep_intrvl}s")

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

    # Enter monitor loop
    old_task_id = None
    try:
        while not shutdown:

            log.info(f"Checking for new tasks with asset '{asset_name}'")
            task_id = odm.get_latest_task_with_asset(project_id, asset_name)

            if not task_id:
                log.warning(f"No task with asset '{asset_name}' found!")
            elif task_id == old_task_id:
                log.info(
                    f"Found same task ({task_id}) as last time, going back to sleep"
                )
            else:
                log.info(f"Found task {task_id}")
                old_task_id = task_id

                # Download
                log.info(f"Downloading asset {asset_name}")
                asset: BytesIO = odm.download_asset(project_id, task_id, asset_name)
                log.debug(f"Downloaded {format_size(getsizeof(asset), binary=True)}")

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

            if sleep_intrvl > 0:
                # sleep in 1sec steps for slightly faster reaction to CTRL+C
                for _ in range(sleep_intrvl):
                    if shutdown:
                        break
                    sleep(1)
                    continue
            else:
                break

    except KeyboardInterrupt:
        log.info("Received keyboard interrupt")

    log.info("ODM->Kafka bridge exiting.")
    return
