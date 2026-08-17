#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import sys
from pathlib import Path

import yaml


class GitLabLoader(yaml.SafeLoader):
    pass


def unknown_tag_handler(loader: GitLabLoader, _tag: str, node: yaml.Node) -> object:
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node)
    if isinstance(node, yaml.MappingNode):
        return loader.construct_mapping(node)
    return loader.construct_scalar(node)


GitLabLoader.add_multi_constructor("!", unknown_tag_handler)


def stages_in(path: Path) -> set[str]:
    stages: set[str] = set()

    for document in yaml.load_all(path.read_text(), Loader=GitLabLoader):
        if not isinstance(document, dict):
            continue

        for definition in document.values():
            if isinstance(definition, dict) and isinstance(definition.get("stage"), str):
                stages.add(definition["stage"])

    return stages


# Check to ensure each gitlab CI file only contains jobs from a single stage
def main() -> int:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".gitlab/ci")
    files = [root] if root.is_file() else sorted(root.rglob("*.gitlab-ci.yml"))
    failed = False

    for path in files:
        stages = stages_in(path)
        if len(stages) > 1:
            print(f"{path}: jobs use multiple stages: {', '.join(sorted(stages))}", file=sys.stderr)
            failed = True

    return int(failed)


if __name__ == "__main__":
    sys.exit(main())
