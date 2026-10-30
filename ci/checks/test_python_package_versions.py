# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Check CTA Python versions through the actual Hatch metadata backend."""

from pathlib import Path
import runpy
import shutil
import subprocess

from hatchling.builders.wheel import WheelBuilder
import pytest

ROOT = Path(__file__).resolve().parents[2]
PACKAGES = ["lib/python/cta-sdk", "lib/python/ctaops-sdk", "lib/python/ctaops-utils", "tools/ctaops-cli"]


@pytest.mark.parametrize("state", ["tag", "dirty", "commit", "untagged"])
@pytest.mark.parametrize("prepared", [False, True])
@pytest.mark.parametrize(
    ("tag", "expected"),
    [
        ("v6.0.0.dev1", "6.0.0.dev1"),
        ("v6.0.0.rc2", "6.0.0rc2"),
        ("v6.0.0", "6.0.0"),
    ],
)
def test_tag_metadata(
    tmp_path: Path,
    tag: str,
    expected: str,
    state: str,
    prepared: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CTA_VERSION_BASE", raising=False)
    monkeypatch.delenv("CI_COMMIT_TAG", raising=False)
    monkeypatch.delenv("CI_PIPELINE_ID", raising=False)
    package_root = tmp_path / PACKAGES[0]
    package_root.mkdir(parents=True)
    shutil.copyfile(ROOT / PACKAGES[0] / "pyproject.toml", package_root / "pyproject.toml")
    helper = tmp_path / "ci/utils/cta_version.py"
    helper.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ROOT / "ci/utils/cta_version.py", helper)

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
    assert WheelBuilder(str(package_root)).metadata.version == f"6.dev123+g{sha}"
    if prepared:
        # Use a different pipeline ID so ignoring prepared metadata fails this assertion.
        monkeypatch.setenv("CTA_VERSION_BASE", f"6.dev456.g{sha}")
        assert WheelBuilder(str(package_root)).metadata.version == f"6.dev456+g{sha}"
    if state in ("tag", "dirty"):
        monkeypatch.setenv("CI_COMMIT_TAG", tag)
        if prepared:
            monkeypatch.setenv("CTA_VERSION_BASE", tag[1:])
        if state == "dirty":
            with pytest.raises(ValueError, match="dirty checkout"):
                _ = WheelBuilder(str(package_root)).metadata.version
        else:
            assert WheelBuilder(str(package_root)).metadata.version == expected
            git("tag", "v7.0.0")
            assert WheelBuilder(str(package_root)).metadata.version == expected
            if prepared:
                monkeypatch.setenv("CTA_VERSION_BASE", "7.0.0")
                with pytest.raises(ValueError, match="does not match CI_COMMIT_TAG"):
                    _ = WheelBuilder(str(package_root)).metadata.version
        monkeypatch.setenv("CI_COMMIT_TAG", "v9.9.9")
        with pytest.raises(ValueError, match="not a valid release tag at HEAD"):
            _ = WheelBuilder(str(package_root)).metadata.version


@pytest.mark.parametrize("invalid", ["", "v6.1.0", "6.1.0.pgall", "6.dev123.gabcdef12.dirty", "not-a-version"])
def test_invalid_prepared_version(invalid: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CTA_VERSION_BASE", invalid)
    version_module = runpy.run_path(str(ROOT / "ci/utils/cta_version.py"))
    with pytest.raises(ValueError, match="Invalid CTA source version"):
        version_module["package_version"]()


@pytest.mark.parametrize("prepared", [False, True])
@pytest.mark.parametrize("tag", ["v7.0.0", "v7.0.0.rc1", "v7.0.0.dev1"])
def test_release_major_mismatch(tmp_path: Path, tag: str, prepared: bool, monkeypatch: pytest.MonkeyPatch) -> None:
    helper = tmp_path / "ci/utils/cta_version.py"
    helper.parent.mkdir(parents=True)
    shutil.copyfile(ROOT / "ci/utils/cta_version.py", helper)
    (tmp_path / "project.json").write_text('{"majorVersion": 6}')
    for args in (
        ["init"],
        ["add", "."],
        ["-c", "user.name=Test", "-c", "user.email=test@example.org", "commit", "-m", "fixture"],
        ["tag", tag],
    ):
        subprocess.run(["git", *args], cwd=tmp_path, check=True, capture_output=True)  # noqa: S603, S607

    monkeypatch.setenv("CI_COMMIT_TAG", tag)
    monkeypatch.delenv("CTA_VERSION_BASE", raising=False)
    if prepared:
        monkeypatch.setenv("CTA_VERSION_BASE", tag[1:])
    version_module = runpy.run_path(str(helper))
    # Both preparation and Python packaging must reject manually created mismatched tags.
    for command in ("source_version", "package_version"):
        with pytest.raises(ValueError, match="does not match majorVersion 6"):
            version_module[command]()


@pytest.mark.parametrize("package", PACKAGES)
def test_package_version_hook(tmp_path: Path, package: str, monkeypatch: pytest.MonkeyPatch) -> None:
    # Each package must resolve the shared helper from its own directory layout.
    package_root = tmp_path / package
    package_root.mkdir(parents=True)
    shutil.copyfile(ROOT / package / "pyproject.toml", package_root / "pyproject.toml")
    helper = tmp_path / "ci/utils/cta_version.py"
    helper.parent.mkdir(parents=True)
    shutil.copyfile(ROOT / "ci/utils/cta_version.py", helper)
    monkeypatch.delenv("CI_COMMIT_TAG", raising=False)
    monkeypatch.setenv("CTA_VERSION_BASE", "6.1.0.rc2")
    assert WheelBuilder(str(package_root)).metadata.version == "6.1.0rc2"


@pytest.mark.parametrize("package", PACKAGES)
def test_archive_metadata_without_git(tmp_path: Path, package: str) -> None:
    shutil.copyfile(ROOT / package / "pyproject.toml", tmp_path / "pyproject.toml")
    (tmp_path / "PKG-INFO").write_text("Metadata-Version: 2.4\nName: fixture\nVersion: 6.0.0rc2\n")
    assert WheelBuilder(str(tmp_path)).metadata.version == "6.0.0rc2"
