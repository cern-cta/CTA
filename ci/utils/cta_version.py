# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Single source of CTA version spelling for release tooling and artifact builds.

No arguments (or ``source``) prints the source version: CI_COMMIT_TAG for a clean
release checkout, <major>.dev<pipeline>.g<sha>[.dirty] for CI, or <major>.dev1 locally.
Hatch calls package_version() for PEP 440 metadata; archive metadata stays with Hatch.

CLI examples (each prints one value):
    python3 ci/utils/cta_version.py major
    python3 ci/utils/cta_version.py image v6.1.0.rc1 el9 pgsched true
    python3 ci/utils/cta_version.py native v6.1.0.rc1 el9 pgsched true

Image tags retain dots; native packages use ~dev/~rc and packaging revision 1.
Backend suffixes belong to the revision; only RPMs also include the platform.
Both artifact commands take a source version and explicit build settings.
They reject artifact tags and do not inspect Git or CI environment variables.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
from pathlib import Path

# Shared by release tooling and artifact builders.
BASE_RE = re.compile(r"^v([1-9][0-9]*)\.([0-9]+)\.([0-9]+)$")
TAG_RE = re.compile(r"^v([1-9][0-9]*)\.([0-9]+)\.([0-9]+)(?:\.(rc|dev)([1-9][0-9]*))?$")
DEVELOPMENT_RE = re.compile(r"^[1-9][0-9]*\.dev[1-9][0-9]*(\.g[a-f0-9]+(\.dirty)?)?$")

PLATFORM_RE = re.compile(r"el[0-9]+|(?:debian|ubuntu)[0-9]+(?:\.[0-9]+)*")
REPOSITORY = Path(__file__).resolve().parents[2]


def major_version(project_file: Path = REPOSITORY / "project.json") -> int:
    """Read the positive integer software series from project.json."""
    major = json.loads(project_file.read_text())["majorVersion"]
    if type(major) is not int or major < 1:
        raise ValueError("majorVersion must be a positive integer")
    return major


def validate_source(version: str) -> None:
    """Require a neutral source version without a v prefix or build suffixes."""
    if not (TAG_RE.fullmatch(f"v{version}") or DEVELOPMENT_RE.fullmatch(version)):
        raise ValueError(f"Invalid CTA source version: {version}")


def build_variant(scheduler: str, oracle: str) -> str:
    """Resolve the backend label from validated build settings."""
    oracle = oracle.lower()
    if scheduler not in {"objectstore", "pgsched"} or oracle not in {"true", "on", "false", "off"}:
        raise ValueError(f"Invalid scheduler/Oracle configuration: {scheduler}/{oracle}")
    if oracle in {"true", "on"}:
        return "pgsched" if scheduler == "pgsched" else ""
    return "pgall" if scheduler == "pgsched" else "pgcat"


def image_version(version: str, platform: str, variant: str) -> str:
    """Format validated source/build inputs as a container image tag."""
    return f"{version}{'.' + variant if variant else ''}.{platform}"


def native_version(version: str, platform: str, variant: str) -> str:
    """Format validated source/build inputs as an RPM or Debian version-release."""
    revision = "1" + (f".{variant}" if variant else "")
    if platform.startswith("el"):
        revision += f".{platform}"
    upstream = version.replace(".dev", "~dev").replace(".rc", "~rc")
    return f"{upstream}-{revision}"


def source_version() -> str:
    """Use explicit CI release tags, identifiable CI builds, or a fixed local version."""

    def git(*args: str) -> str:
        return subprocess.check_output(["git", *args], cwd=REPOSITORY, text=True).strip()  # noqa: S603, S607

    requested = os.environ.get("CI_COMMIT_TAG")
    if requested:
        tags = git("tag", "--points-at", "HEAD").splitlines()
        if requested not in tags or not TAG_RE.fullmatch(requested):
            raise ValueError(f"CI_COMMIT_TAG is not a valid release tag at HEAD: {requested}")
        if git("status", "--porcelain", "--untracked-files=normal"):
            raise ValueError("Cannot build a release version from a dirty checkout")
        return requested[1:]

    major = major_version()
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


def main() -> None:
    """Print one requested version; report invalid inputs without a traceback."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command")
    commands.add_parser("source", help="resolve the source version from CI metadata or local defaults")
    major = commands.add_parser("major", help="read the software series from project.json")
    major.add_argument("project_file", nargs="?", type=Path, default=REPOSITORY / "project.json")
    for name, help_text in [("image", "format an image tag"), ("native", "format an RPM or Debian version-release")]:
        artifact = commands.add_parser(name, help=help_text)
        artifact.add_argument("version", help="neutral source version, with optional v prefix; no build suffixes")
        artifact.add_argument("platform", help="target platform, e.g. el9 or debian13")
        artifact.add_argument("scheduler", choices=["objectstore", "pgsched"])
        artifact.add_argument("oracle", help="Oracle support: true/false or on/off (case-insensitive)")
    args = parser.parse_args()
    try:
        if args.command == "major":
            print(major_version(args.project_file))
        elif args.command in {"image", "native"}:
            # Validate once at the CLI boundary; formatters only render known inputs.
            version = args.version.removeprefix("v")
            validate_source(version)
            if not PLATFORM_RE.fullmatch(args.platform):
                parser.error(f"Invalid CTA platform: {args.platform}")
            variant = build_variant(args.scheduler, args.oracle)
            formatter = image_version if args.command == "image" else native_version
            print(formatter(version, args.platform, variant))
        else:
            print(source_version())
    except (ValueError, OSError, KeyError, subprocess.CalledProcessError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
