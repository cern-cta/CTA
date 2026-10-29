# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Shared Python package version mapping for CTA source tags."""

from __future__ import annotations

from pathlib import Path

from setuptools_scm import ScmVersion, get_version
from setuptools_scm.version import guess_next_dev_version

TAG_REGEX = r"^v(?P<version>[1-9][0-9]*\.[0-9]+\.[0-9]+(?:\.(?:dev|rc)[1-9][0-9]*)?)$"


def development_version(version: ScmVersion) -> str:
    """Allow commits after numbered dev tags, which the default SCM scheme rejects."""
    if not version.exact and version.tag.dev is not None:
        # The SCM local suffix distinguishes snapshots without overtaking the next dev tag.
        return version.format_with("{tag}")
    return guess_next_dev_version(version)


def package_version() -> str:
    """Read checkout versions; Hatch uses PKG-INFO directly for source distributions."""
    repository = Path(__file__).resolve().parents[1]
    return get_version(
        root=str(repository),
        tag_regex=TAG_REGEX,
        version_scheme=development_version,
        # Old packaging tags must not become the baseline for neutral Python versions.
        git_describe_command="git describe --dirty --tags --long --match v[0-9]*.[0-9]*.[0-9]* --exclude *-*",
    )
