#!/usr/bin/python3

# SPDX-FileCopyrightText: 2025 CERN
#
# SPDX-License-Identifier: GPL-3.0-or-later


from pathlib import Path
import json
import argparse
import sys

project_json_path = Path(__file__).resolve().parents[2] / "project.json"
with open(project_json_path, "r") as f:
    project_json = json.load(f)

parser = argparse.ArgumentParser()
parser.add_argument('--platform', default=project_json["dev"]["defaultPlatform"])
args = parser.parse_args()

platform = args.platform

if platform not in project_json["platforms"]:
    valid_platforms = list(project_json["platforms"].keys())
    print(f"Error: invalid platform '{platform}'. Supported options are: {valid_platforms}", file=sys.stderr)
    sys.exit(1)

packages = project_json["platforms"][platform]["buildRequires"]

for package, version in packages.items():
    if package in project_json["packageGroups"]:
        for package_name in project_json["packageGroups"][package]:
            print(f"BuildRequires: {package_name} {version}")
    else:
        print(f"BuildRequires: {package} {version}")
