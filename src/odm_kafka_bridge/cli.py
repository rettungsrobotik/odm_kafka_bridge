#!/usr/bin/env python

from odm_kafka_bridge.bridge import run_bridge

from argparse import ArgumentParser, Namespace
from dotenv import load_dotenv
import logging
import os
from pathlib import Path
import toml
from typing import Any


def main():

    args = parse_args()

    # Configure logger
    log_fmt = "[{asctime}] [{levelname}] [{name}] {message}"
    logging.basicConfig(format=log_fmt, style="{")
    log = logging.getLogger("cli")
    log_lvl = logging.DEBUG if args.debug else logging.INFO
    log.setLevel(log_lvl)

    # Load configuration fom toml file
    toml_path = args.config / "config.toml"
    log.info(f"Loading configuration from {toml_path}")
    config = load_config(toml_path.resolve())

    # Load ODM credentials from .env
    dotenv_path = args.config / ".env"
    log.info(f"Loading credentials from {dotenv_path}")
    load_dotenv(dotenv_path)
    odm_username = os.getenv("ODM_USERNAME")
    odm_password = os.getenv("ODM_PASSWORD")
    if not odm_username or not odm_password:
        raise EnvironmentError("Missing WebODM credentials. Please check .env file.")

    # Load Kafka SSL credentials if authentication configured
    kafka_username, kafka_password, kafka_ssl_key_pw = None, None, None
    if config["kafka"].get("auth") is not None:
        kafka_username = os.getenv("KAFKA_USERNAME")
        kafka_password = os.getenv("KAFKA_PASSWORD")
        kafka_ssl_key_pw = os.getenv("KAFKA_SSL_KEY_PASSWORD")
        if not kafka_username or not kafka_password or not kafka_ssl_key_pw:
            raise EnvironmentError(
                "Kafka authentication configured but credentials not found. Please check your .env."
            )

    try:
        run_bridge(
            config=config,
            odm_user=odm_username,
            odm_password=odm_password,
            kafka_user=kafka_username,
            kafka_password=kafka_password,
            kafka_ssl_key_pw=kafka_ssl_key_pw,
            debug=args.debug,
        )
    except Exception as e:
        log.error(f"{e}")
        if args.debug:
            import pdb

            # auto-drop into debugger
            pdb.post_mortem()


def parse_args() -> Namespace:
    """
    Parses the commandline arguments with argparse.

    Returns:
        Namespace with parsed arguments
    """

    parser = ArgumentParser(description="ODM->Kafka bridge")

    # Required
    parser.add_argument(
        "-c",
        "--config",
        type=Path,
        metavar="dir",
        required=True,
        help="Path to directory containing config.toml and .env",
    )

    # Optional
    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        default=False,
        help="(optional) Enable debug mode (default: %(default)s).\
        Increases verbosity and automatically drops into a debugger if an error occured.",
    )
    return parser.parse_args()


def load_config(path: Path) -> dict[str, Any]:
    """
    Loads configuration parameters from TOML file.

    Args:
        path: Path to config.toml

    Returns:
        Configuration parameters.

    Raises:
        FileNotFoundError
    """

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    return toml.load(str(path))


if __name__ == "__main__":
    """CLI entry point."""
    main()
