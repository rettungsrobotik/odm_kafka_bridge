#!/usr/bin/env python

from odm_kafka_bridge.odm_client import ODMClient
from odm_kafka_bridge.kafka_client import KafkaClient

from io import BytesIO
import logging
from signal import signal, Signals, SIGINT, SIGTERM
from time import sleep
from typing import Optional

shutdown = False  # global flag set by signal handlers


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

    # Register signal handlers

    def handle_signal(signum, _):
        """
        Handles system signals for graceful shutdowns.

        Note: Kafka producer does not seem to react to this properly, so shutdown will
        happen after the message callback. This can take a moment, especially if an
        error occured.

        Args:
            signum: Signal number
        """
        global shutdown
        shutdown = True
        log.info(f"Received signal {signum} ({Signals(signum).name})")
        return

    signal(SIGINT, handle_signal)  # CTRL+C
    signal(SIGTERM, handle_signal)  # systemctl stop

    # Prepare Kafka client
    kafka = KafkaClient(config, kafka_user, kafka_password, kafka_ssl_key_pw, debug)
    kafka.verify_connection()

    # Prepare WebODM client
    odm_url = config["webodm"]["url"]
    project_name = config["webodm"]["project"]
    asset_name = config["webodm"]["asset"]
    odm = ODMClient(odm_url, odm_user, odm_password, debug)
    odm.authenticate()

    log.info("Starting ODM->Kafka bridge")

    # Prepare sleep interval for monitor mode
    sleep_intrvl = int(config["bridge"].get("monitor_interval_sec", -1))
    if sleep_intrvl < 0:
        log.info("Running in oneshot mode")
    else:
        log.info(f"Will check for fresh assets every {sleep_intrvl}s")

    # Find project and task with asset
    project_id = odm.get_project_id_by_name(project_name)

    # main loop
    last_iter_task_id = None
    topic = config["kafka"]["topic"]
    key = config["kafka"]["key"]
    try:
        while not shutdown:

            task_id = odm.get_latest_task_with_asset(project_id, asset_name)

            if not task_id:
                log.warning("Could not find task ID.")
            elif task_id == last_iter_task_id:
                log.info(f"It's the same task as last time, going back to sleep")
            elif kafka.has_been_produced_already(task_id, asset_name, topic, key):
                log.info(
                    f"Message with matching task ID, asset name and key was already produced to Kafka, probably in a previous run."
                )
            else:
                # Send the asset!

                if shutdown:  # in case CTRL+C was pressed during kafka check
                    break

                # Download
                asset: BytesIO = odm.download_asset(project_id, task_id, asset_name)
                if shutdown:  # in case CTRL+C was pressed during download
                    break

                # Upload
                headers = [
                    ("project_name", str(project_name)),
                    ("project_id", str(project_id)),
                    ("task_id", str(task_id)),
                    ("asset_name", str(asset_name)),
                ]
                kafka.produce(asset, headers, topic, key)

            last_iter_task_id = task_id

            # sleep in small steps for faster reaction to shutdown request
            if sleep_intrvl > 0 and not shutdown:
                for _ in range(sleep_intrvl):
                    if shutdown:
                        break
                    sleep(0.5)
            else:
                # running in oneshot mode, or shutdown requested
                break

    except KeyboardInterrupt:
        log.info("Received keyboard interrupt")
    finally:
        kafka.close()

    log.info("ODM->Kafka bridge exiting.")
    return
