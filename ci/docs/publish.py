# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Publish one CTA release without regressing existing documentation versions."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile
from typing import Any

CANONICAL_TAG = re.compile(r"v(\d+)\.(\d+)\.(\d+)(?:\.(\d+))?-(\d+)")
SERIES = re.compile(r"(\d+)\.(\d+)")


def release_key(tag: str) -> tuple[int, ...]:
    """Normalize three/four-component tags before comparing the package revision."""
    match = CANONICAL_TAG.fullmatch(tag)
    if not match:
        raise ValueError(f"Not a canonical release tag: {tag}")
    return tuple(int(part or 0) for part in match.groups())


def publication_decision(tag: str, commit: str, versions: list[dict[str, Any]]) -> tuple[str, bool, str]:
    key = release_key(tag)
    series = f"{key[0]}.{key[1]}"
    publish = True
    numeric_series = [series]
    for version in versions:
        name = version["version"]
        if SERIES.fullmatch(name):
            numeric_series.append(name)
        if name != series:
            continue

        # Refuse to replace a numeric series whose provenance is unknown.
        properties = version.get("properties", {})
        previous = release_key(properties["source_tag"])
        if previous == key and properties["source_commit"] != commit:
            raise ValueError(f"Release {tag} was already published from a different commit")
        publish = key >= previous

    latest = max(numeric_series, key=lambda name: tuple(map(int, name.split("."))))
    return series, publish, latest


def run(*args: str) -> str:
    # Arguments are passed directly to trusted Git/mike executables without a shell.
    return subprocess.check_output(args, text=True).strip()  # noqa: S603


def publish(tag: str, remote: str, branch: str) -> None:
    release_key(tag)
    commit = run("git", "rev-parse", "HEAD")
    if run("git", "rev-parse", f"refs/tags/{tag}^{{commit}}") != commit:
        raise ValueError("The checkout does not match the release tag")

    # CI serializes this entire operation, including fetching and exporting Pages.
    run("git", "fetch", "--no-tags", remote, f"refs/heads/{branch}")
    run("git", "update-ref", f"refs/heads/{branch}", run("git", "rev-parse", "FETCH_HEAD"))
    versions = json.loads(run("git", "show", f"{branch}:public/versions.json"))
    series, should_publish, latest = publication_decision(tag, commit, versions)
    common = [
        "--config-file",
        "docs/mkdocs.yml",
        "--branch",
        branch,
        "--remote",
        remote,
        "--deploy-prefix",
        "public",
    ]
    if should_publish:
        run(
            "mike",
            "deploy",
            *common,
            "--alias-type",
            "redirect",
            "--prop-set-string",
            f"source_tag={tag}",
            "--prop-set-string",
            f"source_commit={commit}",
            series,
        )
        run("mike", "alias", *common, "--update-aliases", "--alias-type", "redirect", latest, "latest")
        run("mike", "set-default", *common, "latest")
        # Publish one branch update after all local mike operations succeed.
        push = ["git", "push"]
        if os.environ.get("GITLAB_CI") == "true":
            push.extend(["--push-option", "ci.skip"])
        run(*push, remote, f"refs/heads/{branch}:refs/heads/{branch}")
    else:
        print(f"Keeping the newer documentation already published for {series}")

    # Export the complete branch even on an older-tag retry, preserving all versions.
    output = Path("build/docs/public")
    with tempfile.TemporaryDirectory() as temp:
        archive = Path(temp) / "pages.tar"
        run("git", "archive", "--format=tar", f"--output={archive}", branch, "public")
        with tarfile.open(archive) as contents:
            contents.extractall(temp, filter="data")
        if output.exists():
            shutil.rmtree(output)
        output.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(Path(temp) / "public"), output)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--remote", default="origin")
    parser.add_argument("--branch", default="gl-pages")
    args = parser.parse_args()
    publish(args.tag, args.remote, args.branch)


if __name__ == "__main__":
    main()
