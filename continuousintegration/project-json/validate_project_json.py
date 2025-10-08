#!/usr/bin/python3

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


from pathlib import Path
import json
import sys
import jsonschema

def validate_schema(project_json, schema_json):
    print(f"Validating project.json using schema.json schema...")

    try:
        jsonschema.validate(instance=project_json, schema=schema_json)
        print("Schema validation of project.json completed successfully.")
        sys.exit(0)
    except jsonschema.ValidationError as e:
        path = ".".join([str(p) for p in e.path]) or "<root>"
        print(f"Schema validation of project.json failed:")
        print(f"  * Path: {path}")
        print(f"  * Error: {e.message}")
        print(f"  * Expected: {e.schema.get('type')}")
        print(f"  * Schema rule: {e.schema}")
        sys.exit(1)

if __name__ == '__main__':
    project_json_path = Path(__file__).resolve().parents[2] / "project.json"
    with open(project_json_path, "r") as f:
        project_json = json.load(f)

    schema_json_path = Path(__file__).resolve().parent / "schema.json"
    with open(schema_json_path, "r") as f:
        schema_json = json.load(f)

    validate_schema(project_json, schema_json)
