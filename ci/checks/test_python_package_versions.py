# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Check CTA Python versions through the actual Hatch metadata backend."""

from pathlib import Path
import shutil
import subprocess

from hatchling.builders.wheel import WheelBuilder
from packaging.version import Version
import pytest

ROOT = Path(__file__).resolve().parents[2]
PACKAGES = ["lib/python/cta-sdk", "lib/python/ctaops-sdk", "lib/python/ctaops-utils", "tools/ctaops-cli"]


@pytest.mark.parametrize("state", ["tag", "dirty", "commit", "untagged"])
@pytest.mark.parametrize("package", PACKAGES)
@pytest.mark.parametrize(
    ("tag", "expected"),
    [
        ("v6.0.0.dev1", "6.0.0.dev1"),
        ("v6.0.0.rc2", "6.0.0rc2"),
        ("v6.0.0", "6.0.0"),
    ],
)
def test_tag_metadata(
    tmp_path: Path, package: str, tag: str, expected: str, state: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CI_COMMIT_TAG", raising=False)
    monkeypatch.delenv("CI_PIPELINE_ID", raising=False)
    package_root = tmp_path / package
    package_root.mkdir(parents=True)
    shutil.copyfile(ROOT / package / "pyproject.toml", package_root / "pyproject.toml")
    helper = tmp_path / "lib/python/package_version.py"
    helper.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ROOT / "lib/python/package_version.py", helper)

    def git(*args: str) -> None:
        subprocess.run(["git", *args], cwd=tmp_path, check=True, capture_output=True)  # noqa: S603, S607

    (tmp_path / "project.json").write_text('{"majorVersion": 6}')
    (tmp_path / ".gitignore").write_text("__pycache__/\n")
    git("init")
    git("add", ".")
    git("-c", "user.name=Test", "-c", "user.email=test@example.org", "commit", "-m", "fixture")
    git("tag", "v5.11.22.0-1")
    git("tag", "v6.0.0-1.dev9")
    if state != "untagged":
        git("tag", tag)
    if state in ("dirty", "commit"):
        (helper).write_text((helper).read_text() + "\n# Changed after the tag.\n")
        if state == "commit":
            git("add", ".")
            git("-c", "user.name=Test", "-c", "user.email=test@example.org", "commit", "-m", "next")
    assert WheelBuilder(str(package_root)).metadata.version == "6.dev1"
    monkeypatch.setenv("CI_PIPELINE_ID", "123")
    sha = subprocess.check_output(["git", "rev-parse", "--short=8", "HEAD"], cwd=tmp_path, text=True).strip()  # noqa: S607
    # A branch pipeline must retain its identity even at a tagged commit.
    assert WheelBuilder(str(package_root)).metadata.version == f"6.dev123+g{sha}{'.dirty' if state == 'dirty' else ''}"
    if state in ("tag", "dirty"):
        monkeypatch.setenv("CI_COMMIT_TAG", tag)
        if state == "dirty":
            with pytest.raises(ValueError, match="dirty checkout"):
                _ = WheelBuilder(str(package_root)).metadata.version
        else:
            assert WheelBuilder(str(package_root)).metadata.version == expected
            git("tag", "v7.0.0")
            assert WheelBuilder(str(package_root)).metadata.version == expected
        monkeypatch.setenv("CI_COMMIT_TAG", "v9.9.9")
        with pytest.raises(ValueError, match="not a valid release tag at HEAD"):
            _ = WheelBuilder(str(package_root)).metadata.version


def test_version_ordering() -> None:
    versions = ["6.0.0.dev1", "6.0.0.dev2", "6.0.0rc1", "6.0.0rc2", "6.0.0", "6.0.1.dev1", "6.0.1"]
    normalized = [Version(version) for version in versions]
    assert all(left < right for left, right in zip(normalized, normalized[1:]))


@pytest.mark.parametrize("package", PACKAGES)
def test_archive_metadata_without_git(tmp_path: Path, package: str) -> None:
    shutil.copyfile(ROOT / package / "pyproject.toml", tmp_path / "pyproject.toml")
    (tmp_path / "PKG-INFO").write_text("Metadata-Version: 2.4\nName: fixture\nVersion: 6.0.0rc2\n")
    assert WheelBuilder(str(tmp_path)).metadata.version == "6.0.0rc2"
