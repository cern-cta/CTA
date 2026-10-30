# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Check CTA Python versions through the actual Hatch metadata backend."""

from __future__ import annotations

import runpy
import shutil
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from hatchling.builders.wheel import WheelBuilder

ROOT = Path(__file__).resolve().parents[2]
PACKAGES = ["lib/python/cta-sdk", "lib/python/ctaops-sdk", "lib/python/ctaops-utils", "tools/ctaops-cli"]


VersionSource = tuple[dict[str, Any], Callable[..., str]]


@pytest.fixture
def version_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> VersionSource:
    for name in ("CI_COMMIT_TAG", "CI_PIPELINE_ID", "CTA_VERSION_BASE"):
        monkeypatch.delenv(name, raising=False)
    helper = tmp_path / "ci/utils/cta_version.py"
    helper.parent.mkdir(parents=True)
    shutil.copyfile(ROOT / "ci/utils/cta_version.py", helper)
    (tmp_path / "project.json").write_text('{"majorVersion": 6}')

    def git(*args: str) -> str:
        return subprocess.check_output(["git", *args], cwd=tmp_path, text=True).strip()  # noqa: S603, S607

    git("init")
    git("add", ".")
    git("-c", "user.name=Test", "-c", "user.email=test@example.org", "commit", "-m", "fixture")
    return runpy.run_path(str(helper)), git


@pytest.mark.parametrize("tag", ["6.0.0", "6.0.0.dev1", "6.0.0.rc2"])
def test_source_selection(version_source: VersionSource, monkeypatch: pytest.MonkeyPatch, tag: str) -> None:
    version, git = version_source
    git("tag", "v" + tag)
    assert version["source_version"]() == "6.dev1"
    monkeypatch.setenv("CI_PIPELINE_ID", "123")
    assert version["source_version"]() == f"6.dev123.g{git('rev-parse', '--short=8', 'HEAD')}"
    monkeypatch.setenv("CI_COMMIT_TAG", "v" + tag)
    assert version["source_version"]() == tag


@pytest.mark.parametrize("prepared", [False, True])
def test_release_checkout_validation(
    version_source: VersionSource, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, prepared: bool
) -> None:
    version, git = version_source
    monkeypatch.setenv("CI_COMMIT_TAG", "v6.0.0")
    if prepared:
        monkeypatch.setenv("CTA_VERSION_BASE", "6.0.0")
    with pytest.raises(ValueError, match="not a valid release tag at HEAD"):
        version["package_version"]()
    git("tag", "v6.0.0")
    assert version["package_version"]() == "6.0.0"
    (tmp_path / "untracked").touch()
    with pytest.raises(ValueError, match="dirty checkout"):
        version["package_version"]()
    monkeypatch.delenv("CI_COMMIT_TAG")
    monkeypatch.delenv("CTA_VERSION_BASE", raising=False)
    assert version["source_version"]() == "6.dev1"
    monkeypatch.setenv("CI_PIPELINE_ID", "123")
    assert version["source_version"]() == f"6.dev123.g{git('rev-parse', '--short=8', 'HEAD')}"


@pytest.mark.parametrize(
    ("prepared", "expected"), [("6.dev456.gabcdef12", "6.dev456+gabcdef12"), ("6.0.0.rc2", "6.0.0rc2")]
)
def test_prepared_version(monkeypatch: pytest.MonkeyPatch, prepared: str, expected: str) -> None:
    monkeypatch.delenv("CI_COMMIT_TAG", raising=False)
    monkeypatch.setenv("CTA_VERSION_BASE", prepared)
    version = runpy.run_path(str(ROOT / "ci/utils/cta_version.py"))
    assert version["package_version"]() == expected


@pytest.mark.parametrize("invalid", ["", "v6.1.0", "6.1.0.pgall", "6.dev123.gabcdef12.dirty", "not-a-version"])
def test_invalid_prepared_version(invalid: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CTA_VERSION_BASE", invalid)
    version = runpy.run_path(str(ROOT / "ci/utils/cta_version.py"))
    with pytest.raises(ValueError, match="Invalid CTA source version"):
        version["package_version"]()


@pytest.mark.parametrize(
    ("tag", "prepared", "error"),
    [
        ("v7.0.0", None, "majorVersion"),
        ("v7.0.0", "7.0.0", "majorVersion"),
        ("v6.0.0", "6.0.1", "does not match CI_COMMIT_TAG"),
    ],
)
def test_release_metadata_mismatch(monkeypatch: pytest.MonkeyPatch, tag: str, prepared: str | None, error: str) -> None:
    version = runpy.run_path(str(ROOT / "ci/utils/cta_version.py"))
    # Git integration is covered above; isolate the metadata comparison here.
    monkeypatch.setitem(version["source_version"].__globals__, "major_version", lambda: 6)

    def git_output(args: list[str], **kwargs: object) -> str:
        return tag if args[1] == "tag" else ""

    monkeypatch.setattr(subprocess, "check_output", git_output)
    monkeypatch.delenv("CTA_VERSION_BASE", raising=False)
    monkeypatch.setenv("CI_COMMIT_TAG", tag)
    if prepared:
        monkeypatch.setenv("CTA_VERSION_BASE", prepared)
    with pytest.raises(ValueError, match=error):
        version["package_version"]()


@pytest.mark.parametrize("package", PACKAGES)
def test_package_version_hook(tmp_path: Path, package: str, monkeypatch: pytest.MonkeyPatch) -> None:
    # Check both the shared helper path and archive fallback for each package.
    package_root = tmp_path / package
    package_root.mkdir(parents=True)
    shutil.copyfile(ROOT / package / "pyproject.toml", package_root / "pyproject.toml")
    helper = tmp_path / "ci/utils/cta_version.py"
    helper.parent.mkdir(parents=True)
    shutil.copyfile(ROOT / "ci/utils/cta_version.py", helper)
    monkeypatch.delenv("CI_COMMIT_TAG", raising=False)
    monkeypatch.setenv("CTA_VERSION_BASE", "6.1.0.rc2")
    assert WheelBuilder(str(package_root)).metadata.version == "6.1.0rc2"
    helper.unlink()
    (package_root / "PKG-INFO").write_text("Metadata-Version: 2.4\nName: fixture\nVersion: 6.0.0rc2\n")
    assert WheelBuilder(str(package_root)).metadata.version == "6.0.0rc2"
