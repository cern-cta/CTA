# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Ensure every explicit GitLab CI before_script includes the merge-train guard."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


GUARD_JOB = ".merge-train-guard"


@dataclass(frozen=True)
class Reference:
    job: str
    key: str


class GitLabLoader(yaml.SafeLoader):
    pass


# Preserve GitLab's cross-file !reference nodes so they can be followed after all CI files are loaded.
def reference_constructor(loader: GitLabLoader, node: yaml.Node) -> Reference:
    if not isinstance(node, yaml.SequenceNode):
        raise TypeError("!reference must be a sequence")
    values = loader.construct_sequence(node)
    if len(values) != 2 or not all(isinstance(value, str) for value in values):
        raise ValueError("!reference must contain a job name and key")
    return Reference(values[0], values[1])


GitLabLoader.add_constructor("!reference", reference_constructor)


def find_ci_files(paths: list[Path]) -> list[Path]:
    files: set[Path] = set()
    for path in paths:
        if path.is_file() and path.name.endswith(".gitlab-ci.yml"):
            files.add(path)
        elif path.is_dir():
            files.update(candidate for candidate in path.rglob("*.gitlab-ci.yml") if candidate.is_file())
    return sorted(files)


def load_jobs(files: list[Path]) -> tuple[dict[str, dict[str, Any]], dict[str, Path]]:
    # Build one lookup across the root configuration and its includes, mirroring GitLab's merged configuration.
    jobs: dict[str, dict[str, Any]] = {}
    origins: dict[str, Path] = {}
    ignored = {"default", "image", "include", "services", "spec", "stages", "variables", "workflow"}
    for path in files:
        for document in yaml.load_all(path.read_text(), Loader=GitLabLoader):
            if not isinstance(document, dict):
                continue
            for name, definition in document.items():
                if name not in ignored and isinstance(definition, dict):
                    jobs[name] = definition
                    origins[name] = path
    return jobs, origins


def references_guard(job_name: str, jobs: dict[str, dict[str, Any]], visited: set[str]) -> bool:
    # Follow before_script references transitively so guarded shared setup templates also satisfy the check.
    if job_name == GUARD_JOB:
        return True
    if job_name in visited:
        return False
    visited.add(job_name)
    before_script = jobs.get(job_name, {}).get("before_script", [])
    if not isinstance(before_script, list):
        return False
    return any(
        isinstance(command, Reference)
        and command.key == "before_script"
        and references_guard(command.job, jobs, visited)
        for command in before_script
    )


def main() -> int:
    paths = [Path(argument) for argument in sys.argv[1:]] or [Path(".gitlab-ci.yml"), Path(".gitlab/ci")]
    files = find_ci_files(paths)
    if not files:
        print("ERROR: no GitLab CI files found", file=sys.stderr)
        return 2

    try:
        jobs, origins = load_jobs(files)
    except Exception as error:
        print(f"ERROR: failed to parse GitLab CI: {error}", file=sys.stderr)
        return 2

    errors = [
        f"{origins[name]}: job '{name}' defines before_script without referencing {GUARD_JOB}"
        for name, definition in jobs.items()
        if name != GUARD_JOB and "before_script" in definition and not references_guard(name, jobs, set())
    ]
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
