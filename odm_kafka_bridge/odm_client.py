#!/usr/bin/env python

from json import loads
import logging
import requests
from tempfile import TemporaryFile
from typing import Optional


class ODMClient:
    """
    Client for downloading assets created by ODM (e.g., DSM) through ODM'S REST API.
    """

    def __init__(
        self, base_url: str, username: str, password: str, debug: bool = False
    ):
        """
        Initialize the ODM client with server credentials.

        Args:
            base_url: Base URL of the WebODM server (e.g. "https://localhost:8000").
            username: ODM username.
            password: ODM password.
            debug: enable debug output.
        """
        self.log = logging.getLogger("odm")
        log_lvl = logging.DEBUG if debug else logging.INFO
        self.log.setLevel(log_lvl)

        access_args = [base_url, username, password]
        assert [
            p is not None and len(p) > 0 for p in access_args
        ], f"URL, username and password may not be empty but at least one ist: {access_args}"

        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.token: Optional[str] = None
        self._debug = debug

        return

    def authenticate(self) -> None:
        """
        Authenticate with the server and store the JWT token.

        NOTE: for security reasons, it is recommended to use a dedicated user with
        minimal permissions.
        """

        self.log.debug(f"Trying to authenticate {self.username} @ {self.base_url}")
        url = f"{self.base_url}/api/token-auth/"
        data = {"username": self.username, "password": self.password}
        # NOTE: Equivalent curl command:
        # curl -X POST -d "username={username}&password=*****" {url}

        response = requests.post(url, data=data, allow_redirects=False)
        response_text = response.text
        response.raise_for_status()
        self.token = loads(response_text)["token"]
        self.log.info("authenticated")

        return

    def _headers(self) -> dict:
        """Helper to return authorization headers."""
        if not self.token:
            raise RuntimeError("Client is not authenticated.")

        return {"Authorization": f"JWT {self.token}"}

    def get_project_id_by_name(self, project_name: str) -> int:
        """
        Fetch the project ID for a given project name.

        Args:
            project_name (str): Name of the project.

        Returns:
            int: The project ID.
        """

        self.log.debug(f"Fetching project ID for {project_name}")
        url = f"{self.base_url}/api/projects"
        response = requests.get(
            url, headers=self._headers(), params={"name": project_name}
        )
        response.raise_for_status()

        resp_json = response.json()
        if not resp_json:
            raise ValueError(f"No project found with name: {project_name}")

        id = resp_json[0]["id"]
        self.log.debug(f"Got project ID: {id}")

        return id

    def get_latest_task_with_asset(
        self, project_id: int, asset_name: str = "dsm.tif"
    ) -> Optional[str]:
        """
        Get the latest task ID in a project that has the requested asset.

        Args:
            project_id: ID of the project.
            asset_name: Desired asset name (e.g. "dsm.tif").

        Returns:
            Optional[str]: Task ID if available, else None.
        """

        self.log.debug(f"Fetching tasks for {project_id} that have {asset_name=}")
        url = f"{self.base_url}/api/projects/{project_id}/tasks"
        response = requests.get(url, headers=self._headers())
        response.raise_for_status()

        resp_json = response.json()
        for task in reversed(resp_json):  # Most recent last
            if asset_name in task.get("available_assets", []):
                id = task["id"]
                self.log.debug(f"Found task with {id=}")
                return task["id"]

        return None

    def download_asset(self, project_id: int, task_id: str, asset_name: str):
        """
        Download a specific asset from a task.

        Args:
            project_id: Project ID.
            task_id: Task ID.
            asset_name: Name of the asset to download.

        Returns:
            tempfile.TemporaryFile file descriptor.
        """

        self.log.info(f"Downloading {asset_name} from {task_id=}")
        url = f"{self.base_url}/api/projects/{project_id}/tasks/{task_id}/download/{asset_name}"
        response = requests.get(url, headers=self._headers(), stream=True)
        response.raise_for_status()

        tmp_file = TemporaryFile()
        for chunk in response.iter_content(chunk_size=8192):
            tmp_file.write(chunk)

        return tmp_file
