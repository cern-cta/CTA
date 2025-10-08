#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


import json
import sys
from pathlib import Path

def compare_commands(map1, map2):
    """
    Compares the commands for each file key in both maps.
    """
    all_files = set(map1.keys()) | set(map2.keys())
    differences = []

    for f in sorted(all_files):
        cmd1 = map1.get(f)
        cmd2 = map2.get(f)
        if cmd1 != cmd2:
            differences.append({
                'file': f,
                'command_file1': cmd1,
                'command_file2': cmd2
            })

    return differences

def compare_files(file1_path, file2_path):
    # Check file existence
    if not file1.exists():
        print(f"ERROR: File '{file1}' not found.")
        sys.exit(1)
    if not file2.exists():
        print(f"ERROR: File '{file2}' not found.")
        sys.exit(1)

    with open(file1_path, 'r', encoding='utf8') as data1, open(file2_path, 'r', encoding='utf-8') as data2:
      map1 = {entry['file']: entry['command'] for entry in json.load(data1)}
      map2 = {entry['file']: entry['command'] for entry in json.load(data2)}

      differences = compare_commands(map1, map2)

    if not differences:
        print("Compilation databases match.")
    else:
        print(f"Compilation databases do not match. Found {len(differences)} mismatches:")
        for diff in differences:
            print(f"\nFile: {diff['file']}")
            print(f"  File1 command: {diff['command_file1']}")
            print(f"  File2 command: {diff['command_file2']}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("ERROR: Incorrect number of arguments")
        print(f"Usage: {Path(sys.argv[0]).name} <file1> <file2>")
        sys.exit(1)

    file1 = Path(sys.argv[1])
    file2 = Path(sys.argv[2])

    compare_files(file1, file2)
