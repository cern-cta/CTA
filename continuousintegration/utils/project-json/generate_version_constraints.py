#!/usr/bin/python3

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2025 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.

import argparse
import json
import re
import sys
from pathlib import Path

def cmake_var_name(package_name: str) -> str:
    """Convert package name to CMake-style upper-case variable name."""
    return re.sub(r'[^A-Z0-9]', '_', package_name.upper())

project_json_path = Path(__file__).resolve().parents[3] / "project.json"
with open(project_json_path, "r") as f:
    project_json = json.load(f)

parser = argparse.ArgumentParser()
parser.add_argument('--platform', default=project_json["dev"]["defaultPlatform"])
args = parser.parse_args()

if args.platform not in project_json["platforms"]:
    valid_platforms = list(project_json["platforms"].keys())
    print(f"Error: invalid platform '{args.platform}'. Supported options are: {valid_platforms}", file=sys.stderr)
    sys.exit(1)

require_versions = project_json["platforms"][args.platform].get("requires", {})

for package, version in require_versions.items():
    var_name = cmake_var_name(package)
    print(f"{var_name}_VERSION_CONSTRAINT=\"{version}\"")
