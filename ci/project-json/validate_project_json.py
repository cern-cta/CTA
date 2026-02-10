#!/usr/bin/python3

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from pathlib import Path
import json
import sys
import re
import jsonschema


def validate_schema(project_json, schema_json):
    print("Validating project.json using schema.json schema...")

    try:
        jsonschema.validate(instance=project_json, schema=schema_json)
        print("Schema validation of project.json completed successfully.")
    except jsonschema.ValidationError as e:
        path = ".".join([str(p) for p in e.path]) or "<root>"
        print("Schema validation of project.json failed:")
        print(f"  * Path: {path}")
        print(f"  * Error: {e.message}")
        print(f"  * Expected: {e.schema.get('type')}")
        print(f"  * Schema rule: {e.schema}")
        sys.exit(1)


def validate_versions(project_json):
    print("Validating version format correctness...")
    errors = 0
    for platform_data in project_json.get("platforms", {}).values():
        versionlock = platform_data.get("versionlock")
        if versionlock is None:
            continue
        for package_name, package_version in versionlock.items():
            # Verify the correctness of the versions under versionlock
            regex = r"^((?P<epoch>[0-9]+):)?(?P<version>[^-]+)-(?P<release>[^-]+(?:\.[^-.]+)*)\.(?P<arch>[^-.]+)$"
            match = re.match(regex, package_version)
            if not match:
                print(f"Version for {package_name} is invalid: {package_version}")
                errors += 1
    if errors > 0:
        print("Valid version format for packages is <version>-<release>.<platform>")
        sys.exit(1)
    print("Version validation completed successfully.")


if __name__ == "__main__":
    project_json_path = Path(__file__).resolve().parents[2] / "project.json"
    with open(project_json_path, "r") as f:
        project_json = json.load(f)

    schema_json_path = Path(__file__).resolve().parent / "schema.json"
    with open(schema_json_path, "r") as f:
        schema_json = json.load(f)

    validate_schema(project_json, schema_json)
    validate_versions(project_json)
