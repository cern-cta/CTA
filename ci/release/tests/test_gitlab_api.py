# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from gitlab_api import GitLabAPI, GitLabAPIError


class GitLabAPITest(unittest.TestCase):
    """Test the mockable GitLab HTTP client behavior."""

    def test_project_id_is_url_encoded(self) -> None:
        api = GitLabAPI("https://gitlab.example", "group/project", "secret")
        assert api.project_url == "https://gitlab.example/api/v4/projects/group%2Fproject"

    def test_paginates(self) -> None:
        api = GitLabAPI("https://gitlab.example", "1", "secret")
        with patch.object(
            api,
            "_request",
            side_effect=[([{"id": 1}], {"x-next-page": "2"}), ([{"id": 2}], {})],
        ) as request:
            assert api.get_all("issues", {"scope": "all"}) == [{"id": 1}, {"id": 2}]
        assert request.call_args_list[0].kwargs["params"]["page"] == 1
        assert request.call_args_list[1].kwargs["params"]["page"] == 2

    def test_rejects_non_list_paginated_response(self) -> None:
        api = GitLabAPI("https://gitlab.example", "1", "secret")
        with patch.object(api, "_request", return_value=({"message": "bad"}, {})):
            with pytest.raises(GitLabAPIError):
                api.get_all("issues")
