# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from ci.checks.check_gitlab_ci_key_order import GitLabLoader
from ci.release.report_job import ReleaseConfig, main, publish_report, render_job


@pytest.fixture
def ci_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in {
        "CI_COMMIT_TAG": "v6.12.3",
        "CI_PIPELINE_ID": "123",
        "CI_PIPELINE_URL": "https://pipeline",
        "CI_PIPELINE_NAME": "tag - el9 - objectstore",
        "CI_API_TOKEN": "token",
    }.items():
        monkeypatch.setenv(name, value)


@pytest.mark.usefixtures("ci_environment")
def test_reports_only_the_corresponding_matrix_instance() -> None:
    name = "publish: [first]"
    with (
        patch("ci.release.report_job.GitLabAPI") as api,
        patch("ci.release.report_job.publish_report") as post,
    ):
        api.return_value.get_all.return_value = [
            {"name": name, "status": "success", "web_url": "https://jobs/1"},
            {"name": "publish: [second]", "status": "running", "web_url": "https://jobs/2"},
        ]
        assert main(["--job", name, "--description", "Publication"]) == 0
    api.return_value.get_all.assert_called_once_with("pipelines/123/jobs", params={"include_retried": "false"})
    assert post.call_args.args[2:5] == ("v6.12.3", "123", "https://pipeline")
    assert "Succeeded: Publication" in post.call_args.args[5]
    assert "objectstore" in post.call_args.args[5]
    assert "second" not in post.call_args.args[5]
    assert post.call_args.args[6] == name


@pytest.mark.usefixtures("ci_environment")
def test_omitted_source_job_does_not_post() -> None:
    with (
        patch("ci.release.report_job.GitLabAPI") as api,
        patch("ci.release.report_job.publish_report") as post,
    ):
        api.return_value.get_all.return_value = []
        assert main(["--job", "absent", "--description", "Publication"]) == 0
    post.assert_not_called()


@pytest.mark.parametrize("status", ["manual", "skipped", "running", "pending"])
def test_unfinished_jobs_do_not_report_completion(status: str) -> None:
    assert render_job({"name": "job", "status": status, "web_url": "https://job"}, "Work") == ""


@pytest.mark.parametrize(
    ("status", "result"), [("success", "Succeeded"), ("failed", "Failed"), ("canceled", "Canceled")]
)
def test_terminal_statuses_are_distinguished(status: str, result: str) -> None:
    report = render_job({"name": "job", "status": status, "web_url": "https://job"}, "Work")
    assert f"{result}: Work" in report
    assert "[job](https://job)" in report


def test_job_retry_updates_its_note_without_replacing_other_results() -> None:
    tag = "v6.12.3"
    api = MagicMock()
    issue = {"iid": 42, "title": "Release v6.12.3"}
    api.get_all.side_effect = [[issue], []]
    api.post.return_value = {"id": "discussion", "notes": []}
    publish_report(api, ReleaseConfig(), tag, "123", "https://pipeline", "Failed", "first")
    heading = api.post.call_args_list[0].kwargs["json"]["body"]
    body = api.post.call_args_list[1].kwargs["json"]["body"]
    assert f"Release pipeline for `{tag}`" in heading
    notes = [{"id": 1, "body": heading}, {"id": 2, "body": body}, {"id": 3, "body": "Human reply"}]

    api.post.reset_mock()
    api.get_all.side_effect = [[issue], [{"id": "discussion", "notes": notes}]]
    publish_report(api, ReleaseConfig(), tag, "123", "https://pipeline", "Other result", "second")
    api.post.assert_called_once()
    api.put.assert_not_called()
    notes.append({"id": 4, "body": api.post.call_args.kwargs["json"]["body"]})

    api.post.reset_mock()
    api.get_all.side_effect = [[issue], [{"id": "discussion", "notes": notes}]]
    publish_report(api, ReleaseConfig(), tag, "123", "https://pipeline", "Succeeded", "first")
    api.post.assert_not_called()
    api.put.assert_called_once()
    assert api.put.call_args.args == ("issues/42/discussions/discussion/notes/2",)
    assert "Succeeded" in api.put.call_args.kwargs["json"]["body"]
    assert "Failed" not in api.put.call_args.kwargs["json"]["body"]


def test_other_pipeline_thread_is_not_reused() -> None:
    api = MagicMock()
    api.get_all.side_effect = [
        [{"iid": 42, "title": "Release v6.12.3"}],
        [{"id": "other", "notes": [{"id": 1, "body": "<!-- cta-release-pipeline:1234 -->"}]}],
    ]
    api.post.return_value = {"id": "new", "notes": []}
    publish_report(api, ReleaseConfig(), "v6.12.3", "123", "https://pipeline", "Results", "job")
    assert api.post.call_args_list[0].args == ("issues/42/discussions",)
    assert api.post.call_args_list[1].args == ("issues/42/discussions/new/notes",)
    api.put.assert_not_called()


@pytest.mark.parametrize("tag", ["v6.12.3.dev1", "v6.12.3.rc1"])
def test_prerelease_without_release_issue_is_skipped(tag: str) -> None:
    api = MagicMock()
    api.get_all.return_value = []
    publish_report(api, ReleaseConfig(), tag, "123", "https://pipeline", "Results", "job")
    api.post.assert_not_called()


def test_missing_release_issue_fails_closed() -> None:
    api = MagicMock()
    api.get_all.return_value = []
    with pytest.raises(RuntimeError, match="Expected one release issue"):
        publish_report(api, ReleaseConfig(), "v6.12.3", "123", "https://pipeline", "Results", "job")
    api.post.assert_not_called()


def test_reporters_depend_on_individual_jobs_including_every_image_instance() -> None:
    root = Path(__file__).resolve().parents[3]
    report = list(yaml.load_all((root / ".gitlab/ci/report.gitlab-ci.yml").read_text(), Loader=GitLabLoader))[-1][
        "report-release"
    ]
    release = list(yaml.load_all((root / ".gitlab/ci/release.gitlab-ci.yml").read_text(), Loader=GitLabLoader))[-1]
    assert [need["job"] for need in report["needs"]] == ["$[[ matrix.REPORT_JOB ]]"]
    jobs = [job for row in report["parallel"]["matrix"] for job in row["REPORT_JOB"]]
    assert len(jobs) == len(set(jobs))
    for publisher in ("publish-images-private", "publish-images-public"):
        targets = release[publisher]["parallel"]["matrix"][0]["TARGET_NAME"]
        assert {job for job in jobs if job.startswith(publisher)} == {f"{publisher}: [{target}]" for target in targets}
