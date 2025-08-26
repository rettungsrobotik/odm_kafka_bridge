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

    # load configuration fom toml file
    config = load_config(args.config / "config.toml")

    # load ODM credentials from .env
    dotenv_path = args.config / ".env"
    load_dotenv(dotenv_path)
    odm_username = os.getenv("ODM_USERNAME")
    odm_password = os.getenv("ODM_PASSWORD")
    kafka_username = os.getenv("KAFKA_USERNAME")
    kafka_password = os.getenv("KAFKA_PASSWORD")
    kafka_ssl_key_pw = os.getenv("KAFKA_SSL_KEY_PASS")
    if not odm_username or not odm_password or not kafka_username or not kafka_password:
        raise EnvironmentError("Missing credentials. Please check .env file.")

    run_bridge(
        config=config,
        odm_user=odm_username,
        odm_password=odm_password,
        kafka_user=kafka_username,
        kafka_password=kafka_password,
        kafka_ssl_key_pw=kafka_ssl_key_pw,
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
        default=Path(__file__).parent / "config",
        help="Path to config dir with toml and certs (default: %(default)s)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Enable debug output (default: %(default)s)",
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
