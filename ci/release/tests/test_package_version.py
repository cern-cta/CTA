# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Check package prerelease ordering independently of container tag spelling."""

# Commands and arguments below are controlled test fixtures.
# ruff: noqa: S603, S607

import shutil
import subprocess
from pathlib import Path

import pytest

VERSION_SCRIPT = Path(__file__).resolve().parents[2] / "utils" / "cta_version.sh"


def package_version(version: str) -> str:
    return subprocess.check_output(
        ["bash", "-c", 'source "$1"; resolve_cta_package_version "$2"', "bash", str(VERSION_SCRIPT), version],
        text=True,
    ).strip()


@pytest.mark.parametrize("suffix", [".el9", ".pgsched.el10", ".pgcat.el9", ".pgall.debian13"])
def test_prerelease_translation(suffix: str) -> None:
    for prerelease in ("dev1", "dev12", "rc1", "rc12"):
        assert package_version(f"6.12.0-1.{prerelease}{suffix}") == f"6.12.0-1~{prerelease}{suffix}"
    assert package_version(f"6.12.0-1{suffix}") == f"6.12.0-1{suffix}"


def test_branch_version_is_preserved() -> None:
    assert package_version("6-dev.123gitabcdef.pgall.el9") == "6-dev.123gitabcdef.pgall.el9"


@pytest.mark.parametrize("manager", ["rpm", "dpkg"])
@pytest.mark.parametrize("suffix", [".el9", ".pgsched.el10", ".pgcat.el9", ".pgall.debian13"])
def test_native_package_ordering(manager: str, suffix: str) -> None:
    if not shutil.which(manager):
        pytest.skip(f"{manager} is not installed")
    versions = [package_version(f"6.12.0-1{pre}{suffix}") for pre in (".dev1", ".dev2", ".rc1", ".rc2", "")]
    for older, newer in zip(versions, versions[1:]):
        if manager == "rpm":
            # The software version is identical; compare the package release fields.
            expression = '%{lua:print(rpm.vercmp("' + older.split("-", 1)[1] + '", "' + newer.split("-", 1)[1] + '"))}'
            assert subprocess.check_output(["rpm", "--eval", expression], text=True).strip() == "-1"
        else:
            subprocess.run(["dpkg", "--compare-versions", older, "lt", newer], check=True)
