#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import argparse
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify that custom dependency inputs were applied to project.json.")
    parser.add_argument("--custom-eos-image-tag", default="")
    parser.add_argument("--custom-xrootd-version", default="")
    parser.add_argument("--platform", required=True)
    return parser.parse_args()


def main() -> None:
    """Validate the effects of custom dependency inputs on project.json."""
    args = parse_args()
    project_json_path = Path(__file__).resolve().parents[2] / "project.json"
    with project_json_path.open() as f:
        project_json = json.load(f)

    if args.custom_xrootd_version:
        project_xrootd_version = project_json["platforms"][args.platform]["versionlock"]["group-xrootd"]
        # Check that at this point the project.json contains the same version
        if args.custom_xrootd_version != project_xrootd_version:
            raise SystemExit(
                "ERROR: custom-xrootd-version must be equal to value in project.json "
                f"({args.custom_xrootd_version} != {project_xrootd_version}). "
                "Please verify the logic in the modify-project-json job."
            )

    project_eos_image_tag = project_json["dev"]["eosImageTag"]
    if args.custom_eos_image_tag and args.custom_eos_image_tag != project_eos_image_tag:
        raise SystemExit(
            "ERROR: custom-eos-image-tag must be equal to value in project.json "
            f"({args.custom_eos_image_tag} != {project_eos_image_tag}). "
            "Please verify the logic in the modify-project-json job."
        )
    print("Validation was successful")


if __name__ == "__main__":
    main()
