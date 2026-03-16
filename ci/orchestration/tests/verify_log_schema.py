# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import argparse
import json
import sys
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", required=True)
    parser.add_argument("--input", default="-")
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument("--print-valid", action="store_true")
    args = parser.parse_args()

    schema = load_schema(args.schema)
    validator = Draft202012Validator(schema)

    errors = 0

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
        elif args.print_valid:
            print(f"line {i}: valid")

    if errors:
        print(f"Total errors found: {errors}")
        sys.exit(1)


if __name__ == "__main__":
    main()
