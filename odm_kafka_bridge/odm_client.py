#!/usr/bin/env python

import requests
from typing import Optional


class ODMClient:
    """
    Client for downloading assets created by ODM (e.g., DSM) through ODM'S REST API.
    """

    def __init__(self, base_url: str, username: str, password: str):
        """
        Initialize the ODM client with server credentials.

        NOTE: for security reasons, creation of a dedicated user account with read-only
        permissions is recommended.

        Args:
            base_url (str): Base URL of the WebODM server (e.g. "https://localhost:8000").
            username (str): ODM username.
            password (str): ODM password.
        """
        assert [p is not None and len(p) > 0 for p in [base_url, username, password]]

        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.token: Optional[str] = None

    def authenticate(self) -> None:
        """Authenticate with the server and store the JWT token."""

        url = f"{self.base_url}/api/token-auth"
        response = requests.post(
            url, data={"username": self.username, "password": self.password}
        )
        response.raise_for_status()
        self.token = response.json()["token"]

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

        url = f"{self.base_url}/api/projects"
        response = requests.get(
            url, headers=self._headers(), params={"name": project_name}
        )
        response.raise_for_status()

        results = response.json()["results"]
        if not results:
            raise ValueError(f"No project found with name: {project_name}")

        return results[0]["id"]

    def get_latest_task_with_asset(
        self, project_id: int, asset_name: str = "dsm.tif"
    ) -> Optional[str]:
        """
        Get the latest task ID in a project that has the requested asset.

        Args:
            project_id (int): ID of the project.
            asset_name (str): Desired asset name (e.g. "dsm.tif").

        Returns:
            Optional[str]: Task ID if available, else None.
        """

        url = f"{self.base_url}/api/projects/{project_id}/tasks"
        response = requests.get(url, headers=self._headers())
        response.raise_for_status()

        tasks = response.json()["results"]
        for task in reversed(tasks):  # Most recent last
            if asset_name in task.get("available_assets", []):
                return task["id"]

        return None

    def download_asset(
        self, project_id: int, task_id: str, asset_name: str, output_path: str
    ) -> None:
        """
        Download a specific asset from a task.

        Args:
            project_id (int): Project ID.
            task_id (str): Task ID.
            asset_name (str): Name of the asset to download.
            output_path (str): Path to save the downloaded file.
        """

        url = f"{self.base_url}/api/projects/{project_id}/tasks/{task_id}/download/{asset_name}"
        response = requests.get(url, headers=self._headers(), stream=True)
        response.raise_for_status()

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
