# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Resolve neutral source versions and their Python packaging spelling."""

from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path

TAG_REGEX = re.compile(r"^v([1-9][0-9]*\.[0-9]+\.[0-9]+(?:\.(?:dev|rc)[1-9][0-9]*)?)$")


def source_version() -> str:
    """Use explicit CI release tags, identifiable CI builds, or a fixed local version."""
    repository = Path(__file__).resolve().parents[2]

    def git(*args: str) -> str:
        return subprocess.check_output(["git", *args], cwd=repository, text=True).strip()  # noqa: S603, S607

    requested = os.environ.get("CI_COMMIT_TAG")
    if requested:
        tags = git("tag", "--points-at", "HEAD").splitlines()
        if requested not in tags or not TAG_REGEX.fullmatch(requested):
            raise ValueError(f"CI_COMMIT_TAG is not a valid release tag at HEAD: {requested}")
        if git("status", "--porcelain", "--untracked-files=normal"):
            raise ValueError("Cannot build a release version from a dirty checkout")
        return requested[1:]

    major = json.loads((repository / "project.json").read_text())["majorVersion"]
    if type(major) is not int or major < 1:
        raise ValueError("majorVersion must be a positive integer")
    build = os.environ.get("CI_PIPELINE_ID")
    if build is None:
        # Local rebuilds intentionally reuse one version, regardless of Git state.
        return f"{major}.dev1"
    if not re.fullmatch(r"[1-9][0-9]*", build):
        raise ValueError("CI_PIPELINE_ID must be a positive integer")
    dirty = bool(git("status", "--porcelain", "--untracked-files=normal"))
    sha = git("rev-parse", "--short=8", "HEAD")
    return f"{major}.dev{build}.g{sha}{'.dirty' if dirty else ''}"


def package_version() -> str:
    """Translate the neutral version to PEP 440; Hatch handles archive metadata."""
    version = source_version()
    return version.replace(".rc", "rc").replace(".g", "+g", 1)


if __name__ == "__main__":
    print(source_version())
