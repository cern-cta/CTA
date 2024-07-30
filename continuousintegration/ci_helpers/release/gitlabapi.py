# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2024 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.

import requests
from typing import Union, Any


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
        params: dict[str, str] | None = None,
        data: Any | None = None,
        json: Any | None = None,
    ) -> Union[Any, None]:
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

    def get(self, endpoint: str, params: dict[str, str] | None = None) -> Union[Any, None]:
        return self.__api_request(endpoint, "GET", params)

    def post(
        self, endpoint: str, params: dict[str, str] | None = None, data: Any | None = None, json: Any | None = None
    ) -> Union[Any, None]:
        return self.__api_request(endpoint, "POST", params, data=data, json=json)

    def put(
        self, endpoint: str, params: dict[str, str] | None = None, data: Any | None = None, json: Any | None = None
    ) -> Union[Any, None]:
        return self.__api_request(endpoint, "PUT", params, data=data, json=json)

    def delete(self, endpoint: str, params: dict[str, str] | None = None) -> Union[Any, None]:
        return self.__api_request(endpoint, "DELETE", params)
