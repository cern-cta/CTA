# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from add_release_note import add_release_note, discussion_marker, main, release_version_for_tag
from release_config import ReleaseConfig


def test_release_version_for_release_candidate_tag() -> None:
    assert release_version_for_tag("v6.12.3.rc3") == "v6.12.3"


def test_new_pipeline_discussion_contains_first_note() -> None:
    api = MagicMock()
    api.get_all.side_effect = [[{"iid": 42, "title": "Release v6.12.3"}], []]

    add_release_note(api, ReleaseConfig(), "v6.12.3.dev1", "123", "https://pipeline", "RPMs published.")

    body = api.post.call_args.kwargs["json"]["body"]
    assert "### Release pipeline for `v6.12.3.dev1`" in body
    assert "RPMs published." in body
    assert discussion_marker("123") in body
    api.post.assert_called_once()


def test_existing_pipeline_discussion_gets_reply() -> None:
    api = MagicMock()
    api.get_all.side_effect = [
        [{"iid": 42, "title": "Release v6.12.3"}],
        [{"id": "discussion-id", "notes": [{"body": discussion_marker("123")}]}],
    ]

    add_release_note(api, ReleaseConfig(), "v6.12.3", "123", "https://pipeline", "Tests passed.")

    api.post.assert_called_once_with(
        "issues/42/discussions/discussion-id/notes",
        json={"body": "Tests passed."},
    )


def test_errors_are_reported_without_failing_the_job(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("CI_COMMIT_TAG", raising=False)

    assert main(["--api-token", "token", "note"]) == 0
    assert "ERROR: Required CI variable CI_COMMIT_TAG is not set" in capsys.readouterr().err


def test_argument_errors_do_not_fail_the_job(capsys: pytest.CaptureFixture[str]) -> None:
    assert main([]) == 0
    assert "the following arguments are required" in capsys.readouterr().err


def test_child_discussion_identifies_artifact_variant(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CTA_IMAGE_TAG", "6.12.3.rc1.pgall.el10")
    api = MagicMock()
    api.get_all.side_effect = [[{"iid": 42, "title": "Release v6.12.3"}], []]
    add_release_note(api, ReleaseConfig(), "v6.12.3.rc1", "123", "https://pipeline", "Published.")
    assert "6.12.3.rc1.pgall.el10" in api.post.call_args.kwargs["json"]["body"]
