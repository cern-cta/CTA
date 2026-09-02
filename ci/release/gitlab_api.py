# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Small, dependency-free GitLab API client used by release tooling."""

from __future__ import annotations

import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

REQUEST_TIMEOUT_SECONDS = 30

Commit = dict[str, Any]


class GitLabAPIError(RuntimeError):
    """An actionable GitLab API failure."""


class GitLabAPI:
    """Provide typed, dependency-free access to project-scoped GitLab APIs."""

    def __init__(self, gitlab_url: str, project_id: str, api_token: str, timeout: float = 30.0):
        """Configure the GitLab project endpoint, token, and request timeout."""
        self.gitlab_url = gitlab_url.rstrip("/")
        self.project_id = project_id
        self.api_token = api_token
        self.timeout = timeout

    @property
    def project_url(self) -> str:
        """Return the URL-encoded project API base URL."""
        return f"{self.gitlab_url}/api/v4/projects/{quote(self.project_id, safe='')}"

    def _request(
        self,
        endpoint: str,
        method: str,
        params: dict[str, Any] | None = None,
        data: Any | None = None,
        json_data: Any | None = None,
        project_scoped: bool = True,
    ) -> tuple[Any, dict[str, str]]:
        """Execute one API request and return its decoded body and headers."""
        api_base_url = self.project_url if project_scoped else f"{self.gitlab_url}/api/v4"
        url = f"{api_base_url}/{endpoint.lstrip('/')}"
        if params:
            url += "?" + urlencode(params, doseq=True)
        body: bytes | None = None
        headers = {"Private-Token": self.api_token, "Accept": "application/json"}
        if json_data is not None:
            body = json.dumps(json_data).encode()
            headers["Content-Type"] = "application/json"
        elif data is not None:
            body = urlencode(data, doseq=True).encode()
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        request = Request(url, data=body, headers=headers, method=method)
        try:
            with urlopen(request, timeout=self.timeout) as response:
                raw = response.read()
                result = json.loads(raw) if raw else None
                return result, {key.lower(): value for key, value in response.headers.items()}
        except HTTPError as error:
            raw = error.read().decode(errors="replace")
            try:
                detail = json.loads(raw).get("message", raw)
            except (json.JSONDecodeError, AttributeError):
                detail = raw
            raise GitLabAPIError(f"{method} {endpoint} failed ({error.code}): {detail}") from error
        except URLError as error:
            raise GitLabAPIError(f"{method} {endpoint} failed: {error.reason}") from error

    def get(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        """Fetch one GitLab API resource."""
        return self._request(endpoint, "GET", params=params)[0]

    def get_all(self, endpoint: str, params: dict[str, Any] | None = None) -> list[Any]:
        """Fetch every page from a list-returning GitLab endpoint."""
        page = 1
        results: list[Any] = []
        while True:
            query = dict(params or {})
            query.update({"page": page, "per_page": 100})
            result, headers = self._request(endpoint, "GET", params=query)
            if not isinstance(result, list):
                raise GitLabAPIError(f"GET {endpoint} returned an object where a list was expected")
            results.extend(result)
            next_page = headers.get("x-next-page")
            if not next_page:
                return results
            page = int(next_page)

    def get_page(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        per_page: int = 20,
    ) -> list[Any]:
        """Fetch one bounded page from a list-returning GitLab endpoint."""
        query = dict(params or {})
        query.update({"page": 1, "per_page": per_page})
        result = self._request(endpoint, "GET", params=query)[0]
        if not isinstance(result, list):
            raise GitLabAPIError(f"GET {endpoint} returned an object where a list was expected")
        return result

    def post(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        data: Any | None = None,
        json: Any | None = None,
    ) -> Any:
        """Create a GitLab API resource."""
        return self._request(endpoint, "POST", params=params, data=data, json_data=json)[0]

    def put(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        data: Any | None = None,
        json: Any | None = None,
    ) -> Any:
        """Update a GitLab API resource."""
        return self._request(endpoint, "PUT", params=params, data=data, json_data=json)[0]

    def delete(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        """Delete a GitLab API resource."""
        return self._request(endpoint, "DELETE", params=params)[0]

    def authenticate(self) -> dict[str, Any]:
        """Verify the configured token and return its GitLab user."""
        result = self._request("user", "GET", project_scoped=False)[0]
        if not isinstance(result, dict):
            raise GitLabAPIError("GitLab authentication returned an unexpected response")
        return result
