#!/usr/bin/env python

from bridge import run_bridge

from argparse import ArgumentParser, Namespace
from dotenv import load_dotenv
import os
from pathlib import Path
import toml


def main():
    args = parse_args()
    load_dotenv()

    config = load_config(args.config)

    odm_url = config["webodm"]["url"]
    project_name = config["webodm"]["project"]
    asset_name = config["webodm"].get("asset", "dsm.tif")

    # kafka_url = config["kafka"]["url"]
    kafka_topic = config["kafka"]["topic"]
    kafka_key = config["kafka"].get("key", "default-key")

    username = os.getenv("ODM_USERNAME")
    password = os.getenv("ODM_PASSWORD")
    if not username or not password:
        raise EnvironmentError("Missing ODM_USERNAME or ODM_PASSWORD in .env")

    run_bridge(
        odm_url=odm_url,
        odm_user=username,
        odm_password=password,
        project_name=project_name,
        asset_name=asset_name,
        kafka_topic=kafka_topic,
        kafka_key=kafka_key,
        debug=args.debug,
    )


def parse_args() -> Namespace:
    """
    Parses the commandline arguments with argparse.

    Returns:
        Namespace with parsed arguments
    """

    parser = ArgumentParser(description="ODM to Kafka bridge CLI")

    parser.add_argument(
        "-c",
        "--config",
        type=Path,
        metavar="path",
        default=Path(__file__).parent / "config.toml",
        help="Path to TOML config file (default: %(default)s)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output (default: false)",
    )
    return parser.parse_args()


def load_config(path: str):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    return toml.load(path)


if __name__ == "__main__":
    main()
