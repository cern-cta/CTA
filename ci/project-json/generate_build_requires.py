#!/usr/bin/python3

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import argparse
import json
import sys
from pathlib import Path

project_json_path = Path(__file__).resolve().parents[2] / "project.json"
with project_json_path.open() as f:
    project_json = json.load(f)

parser = argparse.ArgumentParser()
parser.add_argument("--platform", default=project_json["dev"]["defaultPlatform"])
args = parser.parse_args()

platform = args.platform

if platform not in project_json["platforms"]:
    valid_platforms = list(project_json["platforms"].keys())
    print(f"Error: invalid platform '{platform}'. Supported options are: {valid_platforms}", file=sys.stderr)
    sys.exit(1)

package_format = "deb" if platform.startswith(("debian", "ubuntu")) else "rpm"

packages = project_json["platforms"][platform]["buildRequires"]

requirements = []
for package, version in packages.items():
    for name in project_json["packageGroups"].get(package, [package]):
        if package_format == "deb":
            requirements.append(f"{name} ({version})" if version else name)
        else:
            requirements.append(f"BuildRequires: {name} {version}".rstrip())

print((",\n " if package_format == "deb" else "\n").join(requirements))
