# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Check artifact mapping and native package ordering from neutral source tags."""

# Commands and arguments below are controlled test fixtures.
# ruff: noqa: S603, S607

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

VERSION_SCRIPT = Path(__file__).resolve().parents[3] / "ci/utils/cta_version.py"


def version_cli(command: str, *arguments: str) -> str:
    return subprocess.check_output(
        [sys.executable, str(VERSION_SCRIPT), command, *arguments],
        text=True,
    ).strip()


@pytest.mark.parametrize(
    ("source", "platform", "scheduler", "oracle", "image", "native"),
    [
        ("v6.1.0", "el9", "objectstore", "true", "6.1.0.el9", "6.1.0-1.el9"),
        ("v6.1.0.rc12", "el10", "pgsched", "true", "6.1.0.rc12.pgsched.el10", "6.1.0~rc12-1.pgsched.el10"),
        ("v6.1.0.dev12", "el9", "objectstore", "false", "6.1.0.dev12.pgcat.el9", "6.1.0~dev12-1.pgcat.el9"),
        ("v6.1.0", "el10", "pgsched", "false", "6.1.0.pgall.el10", "6.1.0-1.pgall.el10"),
        ("v6.1.0", "debian13", "objectstore", "true", "6.1.0.debian13", "6.1.0-1"),
        ("v6.1.0.dev1", "debian13", "pgsched", "false", "6.1.0.dev1.pgall.debian13", "6.1.0~dev1-1.pgall"),
        ("v6.1.0.rc1", "ubuntu24.04", "pgsched", "true", "6.1.0.rc1.pgsched.ubuntu24.04", "6.1.0~rc1-1.pgsched"),
    ],
)
def test_artifact_mapping(source: str, platform: str, scheduler: str, oracle: str, image: str, native: str) -> None:
    assert version_cli("image", source, platform, scheduler, oracle) == image
    assert version_cli("native", source, platform, scheduler, oracle) == native


@pytest.mark.parametrize(
    ("source", "platform", "expected"),
    [
        ("6.dev1", "el9", "6~dev1-1.pgall.el9"),
        ("6.dev1", "debian13", "6~dev1-1.pgall"),
        ("6.dev123.gabcdef", "el9", "6~dev123.gabcdef-1.pgall.el9"),
        ("6.dev123.gabcdef", "debian13", "6~dev123.gabcdef-1.pgall"),
    ],
)
def test_branch_version(source: str, platform: str, expected: str) -> None:
    assert version_cli("native", source, platform, "pgsched", "false") == expected


@pytest.mark.parametrize(
    "version",
    [
        "v6.1.0-1",
        "v6.1.0.rc0",
        "v6.1.0.dev1.rc1",
        "v6.1.0.pgall",
        "v6.1.0.el10",
        "v6.01.0",
        "v6.1.00",
        "6.dev123.gabcdef12.dirty",
    ],
)
def test_invalid_source_versions(version: str) -> None:
    with pytest.raises(subprocess.CalledProcessError):
        version_cli("image", version, "el9", "objectstore", "true")


@pytest.mark.parametrize(("manager", "platform"), [("rpm", "el9"), ("dpkg", "debian13")])
def test_native_package_ordering(manager: str, platform: str) -> None:
    if not shutil.which(manager):
        pytest.skip(f"{manager} is not installed")
    versions = [
        version_cli("native", version, platform, "pgsched", "false")
        for version in ["6.1.0.dev1", "6.1.0.dev2", "6.1.0.rc1", "6.1.0.rc2", "6.1.0", "6.1.1"]
    ]
    for older, newer in zip(versions, versions[1:]):
        if manager == "rpm":
            # Prerelease status belongs to Version; Release stays at packaging revision 1.
            expression = '%{lua:print(rpm.vercmp("' + older.split("-", 1)[0] + '", "' + newer.split("-", 1)[0] + '"))}'
            assert subprocess.check_output(["rpm", "--eval", expression], text=True).strip() == "-1"
        else:
            subprocess.run(["dpkg", "--compare-versions", older, "lt", newer], check=True)
