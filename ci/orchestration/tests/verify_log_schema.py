# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import argparse
import json
import sys

from collections import defaultdict
from jsonschema import Draft202012Validator


def load_schema(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def iter_lines(path):
    if path == "-":
        for line in sys.stdin:
            yield line
    else:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                yield line


def extract_expected_events(schema, program_name):
    expected_events = set()
    try:
        enum_events = schema["properties"]["event_name"]["enum"]
        expected_events.update(enum_events)
    except KeyError:
        pass

    # Not all events may occur in the output logs of test
    ignore_events = ["program_exiting"]

    # Some events only occur in particular programs. Build a map of which event occurs in which program
    event_program_map = defaultdict(list)
    if "allOf" in schema:
        for condition in schema["allOf"]:
            try:
                event_const = condition["if"]["properties"]["event_name"]["const"]

                if event_const not in expected_events:
                    print(
                        f'ERROR: Log schema incomplete. Event name "{event_const}" does not appear in event_name enum'
                    )
                    sys.exit(1)
                if "program" in condition["if"]["properties"]:
                    program_const = condition["if"]["properties"]["program"]["const"]
                    event_program_map[event_const].append(program_const)
            except KeyError:
                continue

    # Ignore events that are not supposed to occur in the current program
    for event_name, program_names in event_program_map.items():
        if program_names and (program_name not in program_names):
            ignore_events.append(event_name)

    return expected_events - set(ignore_events)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", required=True)
    parser.add_argument("--program-name", required=True)
    parser.add_argument("--input", default="-")
    parser.add_argument("--fail-fast", action="store_true")
    args = parser.parse_args()

    print(f"Verifying log schema for program: {args.program_name}")

    schema = load_schema(args.schema)
    validator = Draft202012Validator(schema)

    expected_events = extract_expected_events(schema, args.program_name)
    observed_events = set()

    errors = 0
    i = 0
    for i, line in enumerate(iter_lines(args.input), start=1):
        line = line.strip()
        if not line:
            continue

        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            print(f"ERROR: Invalid JSON found on line {i}")
            print(f"  * Contents: {line}")
            errors += 1
            if args.fail_fast:
                sys.exit(1)
            continue

        if "event_name" in obj:
            observed_events.add(obj["event_name"])

        violations = sorted(validator.iter_errors(obj), key=lambda e: e.path)

        if violations:
            errors += 1
            print(f"ERROR: Schema violation found on line {i}")
            print(f"  * Contents: {line}")
            print("  * Violations:")
            for v in violations:
                path = ".".join(map(str, v.path))
                if path:
                    print(f"      {path}: {v.message}")
                else:
                    print(f"      {v.message}")
            if args.fail_fast:
                sys.exit(1)

    missing_events = expected_events - observed_events
    if missing_events:
        print("\nERROR: Test coverage incomplete!")
        print("The schema expects coverage for these events, but they were missing from the input logs:")
        for missing in sorted(missing_events):
            print(f"  - {missing}")
        errors += 1

    if errors:
        print(f"Total errors found: {errors}")
        sys.exit(1)

    if i == 0:
        print(f"ERROR: No JSON objects found in {args.input}")
        sys.exit(1)

    print(f"SUCCESS: Verification of {args.input} against schema {args.schema} passed. {i} lines checked.")
    print(f"Coverage complete: All {len(expected_events)} defined event types were tested.")


if __name__ == "__main__":
    main()
