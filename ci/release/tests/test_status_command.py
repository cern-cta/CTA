# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Status remains useful throughout preparation and candidate validation."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from commands import status


@pytest.mark.parametrize("state", [None, "opened", "merged"])
def test_status_before_tagging(state: str | None, capsys: pytest.CaptureFixture[str]) -> None:
    context = MagicMock()
    context.find_or_create_release_issue.return_value = None
    context.find_changelog_merge_request.return_value = (
        {"state": state, "web_url": "mr-url", "merge_commit_sha": "merge"} if state else None
    )
    context.api.get_all.return_value = []
    status.run(context, "v6.12.0")
    output = capsys.readouterr().out
    assert "Tags:     not published" in output
    assert (state or "not created") in output
    context.find_pipeline.assert_not_called()
    context.api.post.assert_not_called()


def test_status_reports_candidate_pipeline_and_manual_variants(capsys: pytest.CaptureFixture[str]) -> None:
    context = MagicMock()
    context.find_or_create_release_issue.return_value = {"web_url": "issue-url"}
    context.find_changelog_merge_request.return_value = None
    context.api.get_all.side_effect = [
        [
            {"name": "v6.12.0.rc2", "commit": {"id": "corrected"}},
            {"name": "v6.12.0-10", "commit": {"id": "other"}},
        ],
        [
            {"name": "pgsched", "downstream_pipeline": {"status": "failed", "web_url": "child-url"}},
            {"name": "el10", "status": "manual"},
        ],
    ]
    context.find_pipeline.return_value = {"id": 42, "status": "failed", "web_url": "tag-url"}
    status.run(context, "v6.12.0.rc2")
    context.find_or_create_release_issue.assert_called_once_with("v6.12.0", create=False)
    context.find_pipeline.assert_called_once_with("corrected", "v6.12.0.rc2", pipeline_source="push")
    output = capsys.readouterr().out
    assert "Tag pipeline: failed tag-url" in output
    assert "pgsched: failed child-url" in output
    assert "el10: manual" in output
    context.api.post.assert_not_called()
