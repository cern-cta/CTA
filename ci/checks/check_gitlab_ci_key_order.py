#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import sys
from pathlib import Path
import yaml

EXPECTED_ORDER = [
    "extends",
    "allow_failure",
    "retry",
    "interruptible",
    "timeout",
    "stage",
    "needs",
    "dependencies",
    "rules",
    "tags",
    "image",
    "variables",
    "before_script",
    "script",
    "after_script",
    "artifacts",
    "release",
]

IGNORED_TOP_LEVEL_KEYS = {
    "stages",
    "include",
    "workflow",
    "default",
    "variables",
    "image",
    "services",
}


# ---- GitLab-aware YAML loader ----------------------------------------------


class GitLabLoader(yaml.SafeLoader):
    pass


def unknown_tag_handler(loader, tag_suffix, node):
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node)
    if isinstance(node, yaml.MappingNode):
        return loader.construct_mapping(node)
    return loader.construct_scalar(node)


GitLabLoader.add_multi_constructor("!", unknown_tag_handler)

# -----------------------------------------------------------------------------


def order_index(key: str) -> int:
    try:
        return EXPECTED_ORDER.index(key)
    except ValueError:
        return len(EXPECTED_ORDER)


def check_job(file_path: Path, job_name: str, job_def: dict) -> list[str]:
    keys = list(job_def.keys())
    sorted_keys = sorted(keys, key=order_index)

    if keys != sorted_keys:
        return [
            f"{file_path}: job '{job_name}' has invalid key order:",
            f"  found:    {keys}",
            f"  expected: {sorted_keys}",
        ]

    return []


def check_gitlab_ci_file(path: Path) -> list[str]:
    try:
        data = yaml.load(path.read_text(), Loader=GitLabLoader)
    except Exception as exc:
        return [f"{path}: failed to parse YAML ({exc})"]

    if not isinstance(data, dict):
        return []

    errors: list[str] = []

    for key, value in data.items():
        if key in IGNORED_TOP_LEVEL_KEYS:
            continue

        if not isinstance(value, dict):
            continue  # anchors, references, or extends-only jobs

        errors.extend(check_job(path, key, value))

    return errors


def find_gitlab_ci_files(root: Path) -> list[Path]:
    def is_gitlab_ci(path: Path) -> bool:
        return path.is_file() and path.name.endswith(".gitlab-ci.yml")

    if root.is_file():
        return [root] if is_gitlab_ci(root) else []

    return [p for p in root.rglob("*.gitlab-ci.yml") if is_gitlab_ci(p)]


def main() -> int:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".")

    files = find_gitlab_ci_files(root)
    if not files:
        print("ERROR: no .gitlab-ci.yml files found", file=sys.stderr)
        return 2

    all_errors: list[str] = []

    for path in files:
        all_errors.extend(check_gitlab_ci_file(path))

    if all_errors:
        print("\n".join(all_errors), file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
