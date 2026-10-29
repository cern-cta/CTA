# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Check artifact mapping and native package ordering from neutral source tags."""

# Commands and arguments below are controlled test fixtures.
# ruff: noqa: S603, S607

import shutil
import subprocess
from pathlib import Path

import pytest

VERSION_SCRIPT = Path(__file__).resolve().parents[2] / "utils" / "cta_version.sh"


def shell_function(function: str, *arguments: str) -> str:
    return subprocess.check_output(
        ["bash", "-c", 'source "$1"; shift; "$@"', "bash", str(VERSION_SCRIPT), function, *arguments],
        text=True,
    ).strip()


@pytest.mark.parametrize("platform", ["el9", "el10", "debian13", "ubuntu24.04"])
@pytest.mark.parametrize(
    ("scheduler", "oracle", "variant"),
    [
        ("objectstore", "true", ""),
        ("pgsched", "true", "pgsched"),
        ("objectstore", "false", "pgcat"),
        ("pgsched", "false", "pgall"),
    ],
)
@pytest.mark.parametrize("prerelease", ["", ".dev1", ".dev12", ".rc1", ".rc12"])
def test_artifact_mapping(platform: str, scheduler: str, oracle: str, variant: str, prerelease: str) -> None:
    source = f"v6.12.0{prerelease}"
    image = f"6.12.0{prerelease}{'.' + variant if variant else ''}.{platform}"
    assert shell_function("resolve_cta_version", source, platform, scheduler, oracle) == image
    assert shell_function("resolve_cta_version", image, platform, scheduler, oracle) == image
    upstream = f"6.12.0{prerelease.replace('.', '~')}"
    revision = (
        f"1{'.' + variant if variant else ''}.{platform}"
        if platform.startswith("el")
        else (f"1{'.' + variant if variant else ''}")
    )
    assert shell_function("resolve_cta_package_version", image) == f"{upstream}-{revision}"


@pytest.mark.parametrize(
    ("image", "expected"),
    [
        ("6.dev1.pgall.el9", "6~dev1-1.pgall.el9"),
        ("6.dev1.pgall.debian13", "6~dev1-1.pgall"),
        ("6.dev1.gabcdef.dirty.pgall.el9", "6~dev1.gabcdef.dirty-1.pgall.el9"),
        ("6.dev123.gabcdef.pgall.el9", "6~dev123.gabcdef-1.pgall.el9"),
        ("6.dev123.gabcdef.pgall.debian13", "6~dev123.gabcdef-1.pgall"),
    ],
)
def test_branch_version(image: str, expected: str) -> None:
    assert shell_function("resolve_cta_package_version", image) == expected


@pytest.mark.parametrize(
    "version",
    [
        "v6.12.0-1",
        "v6.12.0.rc0",
        "v6.12.0.dev1.rc1",
        "v6.12.0.pgall",
        "v6.12.0.el10",
    ],
)
def test_invalid_or_mismatched_image_versions(version: str) -> None:
    with pytest.raises(subprocess.CalledProcessError):
        shell_function("resolve_cta_version", version, "el9", "objectstore", "true")


@pytest.mark.parametrize(("manager", "platform"), [("rpm", "el9"), ("dpkg", "debian13")])
@pytest.mark.parametrize("variant", ["", ".pgsched", ".pgcat", ".pgall"])
def test_native_package_ordering(manager: str, platform: str, variant: str) -> None:
    if not shutil.which(manager):
        pytest.skip(f"{manager} is not installed")
    versions = [
        shell_function("resolve_cta_package_version", f"{version}{variant}.{platform}")
        for version in ["6.12.0.dev1", "6.12.0.dev2", "6.12.0.rc1", "6.12.0.rc2", "6.12.0", "6.12.1"]
    ]
    for older, newer in zip(versions, versions[1:]):
        if manager == "rpm":
            # Prerelease status belongs to Version; Release stays at packaging revision 1.
            expression = '%{lua:print(rpm.vercmp("' + older.split("-", 1)[0] + '", "' + newer.split("-", 1)[0] + '"))}'
            assert subprocess.check_output(["rpm", "--eval", expression], text=True).strip() == "-1"
        else:
            subprocess.run(["dpkg", "--compare-versions", older, "lt", newer], check=True)
