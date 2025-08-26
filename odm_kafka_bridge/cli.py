#!/usr/bin/env python

from bridge import run_bridge

from argparse import ArgumentParser, Namespace
from dotenv import load_dotenv
import os
from pathlib import Path
import toml
from typing import Any


def main():
    args = parse_args()

    # load configuration fom a toml file
    config = load_config(args.config)
    odm_url = args.odm_url if args.odm_url else config["webodm"]["url"]
    project_name = config["webodm"]["project"]
    asset_name = config["webodm"].get("asset", "dsm.tif")
    kafka_url = config["kafka"]["url"]
    kafka_topic = config["kafka"]["topic"]
    kafka_key = config["kafka"].get("key", "default-key")

    # load ODM credentials from .env
    dotenv_path = Path(__file__).parent / ".env"
    load_dotenv(dotenv_path)
    username = os.getenv("ODM_USERNAME")
    password = os.getenv("ODM_PASSWORD")
    if not username or not password:
        raise EnvironmentError(
            "Missing ODM credentials. Please set ODM_USERNAME and ODM_PASSWORD in a .env file."
        )

    run_bridge(
        odm_url=odm_url,
        odm_user=username,
        odm_password=password,
        project_name=project_name,
        asset_name=asset_name,
        kafka_url=kafka_url,
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
    parser.add_argument(
        "--odm_url",
        type=str,
        metavar="str",
        required=False,
        help="Override WebODM URL from config",
    )
    return parser.parse_args()


def load_config(path: Path) -> dict[str, Any]:
    """
    Loads configuration parameters from TOML file.

    Args:
        path (pathlib.Path): Path to config.toml

    Returns:
        Configuration parameters

    Raises:
        FileNotFoundError
    """

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    return toml.load(str(path))


if __name__ == "__main__":
    """CLI entry point."""
    main()
