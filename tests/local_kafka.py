#!/usr/bin/env python

import subprocess
import time


class LocalKafkaStack:
    """
    Spins up or tears down a local Kafka cluster to run unit tests, using docker compose.
    Skips up and down if it detects kafka and zookepper to be already running.
    """

    def __init__(self, cwd="tests"):
        """Constructor"""
        self.compose_cmd = ["docker", "-l", "error", "compose"]
        self.compose_cwd = cwd
        self.did_start_kafka = False

    def _is_kafka_running(self) -> bool:
        """
        Checks docker ps for already running Kafka containers.

        Returns:
        - true if containers detected, false otherwise.
        """

        try:
            result = subprocess.run(
                self.compose_cmd + ["ps", "--status", "running", "--services"],
                cwd=self.compose_cwd,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                encoding="utf-8",
            )
            services = result.stdout.strip().splitlines()
            return "kafka" in services and "zookeeper" in services
        except Exception:
            return False

    def start(self) -> None:
        """
        Starts the local cluster, if not started already.
        """

        if self._is_kafka_running():
            print("🔁 Kafka stack already running, skipping 'up'")
            self.did_start_kafka = False
            return

        cmd = self.compose_cmd + ["up", "-d", "--force-recreate"]
        subprocess.run(cmd, cwd=self.compose_cwd, check=True)
        time.sleep(15)
        print("🟢 Kafka stack started.")
        self.did_start_kafka = True

        return

    def stop(self) -> None:
        """
        Stops the local cluster, if started by us and not stopped already.
        """

        if not self.did_start_kafka:
            print("⏹️ Kafka was not started by this test run, skipping 'down'")
            return
        elif not self._is_kafka_running():
            print("❌ Kafka seems to have already been stopped!")
            return

        cmd = self.compose_cmd + ["down"]
        subprocess.run(cmd, cwd=self.compose_cwd, check=True)
        self.did_start_kafka = False
        print("🔴 Kafka stack stopped.")
        return
