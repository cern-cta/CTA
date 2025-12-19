#!/usr/bin/python3

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import argparse
import json
import re
import sys
from pathlib import Path


def cmake_var_name(package_name: str) -> str:
    """Convert package name to CMake-style upper-case variable name."""
    return re.sub(r"[^A-Z0-9]", "_", package_name.upper())


project_json_path = Path(__file__).resolve().parents[2] / "project.json"
with open(project_json_path, "r") as f:
    project_json = json.load(f)

parser = argparse.ArgumentParser()
parser.add_argument("--platform", default=project_json["dev"]["defaultPlatform"])
args = parser.parse_args()

if args.platform not in project_json["platforms"]:
    valid_platforms = list(project_json["platforms"].keys())
    print(f"Error: invalid platform '{args.platform}'. Supported options are: {valid_platforms}", file=sys.stderr)
    sys.exit(1)

require_versions = project_json["platforms"][args.platform].get("runRequires", {})

for package, version in require_versions.items():
    var_name = cmake_var_name(package)
    print(f'{var_name}_VERSION_CONSTRAINT="{version}"')
