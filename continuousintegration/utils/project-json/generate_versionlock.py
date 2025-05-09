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

from pathlib import Path
import json
import argparse
import sys

project_json_path = Path(__file__).resolve().parents[3] / "project.json"
with open(project_json_path, "r") as f:
    project_json = json.load(f)

parser = argparse.ArgumentParser()
parser.add_argument('--platform', default=project_json["defaultPlatform"])
args = parser.parse_args()

platform = args.platform

if platform not in project_json["platforms"]:
    valid_platforms = list(project_json["platforms"].keys())
    print(f"Error: invalid platform '{platform}'. Supported options are: {valid_platforms}", file=sys.stderr)
    sys.exit(1)

packages = project_json["platforms"][platform]["versionlock"]
arch = project_json["platforms"][platform]["arch"]

for package, version in packages.items():
    if package in project_json["packageGroups"]:
        for package_name in project_json["packageGroups"][package]:
            print(f"{package_name}-{version}.{platform}.{arch}")
    else:
        print(f"{package}-{version}.{platform}.{arch}")
