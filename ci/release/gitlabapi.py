# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import requests
from typing import Any, Optional


Commit = dict[str, Any]


class GitLabAPI:

    gitlab_url: str
    project_id: str
    api_token: str

    def __init__(self, gitlab_url: str, project_id: str, api_token: str):
        self.gitlab_url = gitlab_url
        self.project_id = project_id
        self.api_token = api_token

    def __api_request(
        self,
        endpoint: str,
        method: str,
        params: Optional[dict[str, str]] = None,
        data: Optional[Any] = None,
        json: Optional[Any] = None,
    ) -> Optional[Any]:
        api_url = f"{self.gitlab_url}/api/v4/projects/{self.project_id}/{endpoint}"
        headers = {
            "Private-Token": self.api_token,
        }
        try:
            if method == "GET":
                response = requests.get(api_url, params=params, headers=headers)
            elif method == "POST":
                response = requests.post(api_url, params=params, headers=headers, data=data, json=json)
            elif method == "PUT":
                response = requests.put(api_url, params=params, headers=headers, data=data, json=json)
            elif method == "DELETE":
                response = requests.delete(api_url, params=params, headers=headers)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            response.raise_for_status()  # Raise an exception for non-success status codes
            return response.json()
        except requests.RequestException as e:
            print(f"ERROR: GitLab API request failed:\n{e}")
            return None

    def get(self, endpoint: str, params: Optional[dict[str, str]] = None) -> Optional[Any]:
        return self.__api_request(endpoint, "GET", params)

    def post(
        self,
        endpoint: str,
        params: Optional[dict[str, str]] = None,
        data: Optional[Any] = None,
        json: Optional[Any] = None,
    ) -> Optional[Any]:
        return self.__api_request(endpoint, "POST", params, data=data, json=json)

    def put(
        self,
        endpoint: str,
        params: Optional[dict[str, str]] = None,
        data: Optional[Any] = None,
        json: Optional[Any] = None,
    ) -> Optional[Any]:
        return self.__api_request(endpoint, "PUT", params, data=data, json=json)

    def delete(self, endpoint: str, params: Optional[dict[str, str]] = None) -> Optional[Any]:
        return self.__api_request(endpoint, "DELETE", params)
