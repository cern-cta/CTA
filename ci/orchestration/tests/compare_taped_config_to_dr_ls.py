#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import argparse
import subprocess
import json
import sys

KEY_SKIP_LIST = ["MountCriteria"]


def run(cmd):
    p = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"cmd failed: {cmd}\n{p.stderr}")
    return p.stdout


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--namespace", required=True)
    args = ap.parse_args()

    ns = args.namespace

    cta_taped_pod = run(
        f'kubectl get pod -l app.kubernetes.io/component=taped -n {ns} --no-headers -o custom-columns=":metadata.name" | head -1'
    )
    cta_cli_pod = run(
        f'kubectl get pod -l app.kubernetes.io/component=cli -n {ns} --no-headers -o custom-columns=":metadata.name" | head -1'
    )

    taped_config = run(f"kubectl -n {ns} exec {cta_taped_pod} -c cta-taped -- " "bash -c 'cat /etc/cta/cta-taped.conf'")

    drive_name = run(f"kubectl -n {ns} exec {cta_taped_pod} -c cta-taped -- printenv DRIVE_NAME").strip()

    drive_json = run(f"kubectl -n {ns} exec {cta_cli_pod} -c cta-cli -- " "cta-admin --json dr ls")

    entries = [e for e in json.loads(drive_json) if e.get("driveName") == drive_name]

    if not entries:
        print("Drive not found")
        sys.exit(1)

    config_json = entries[0].get("driveConfig")
    if config_json is None:
        print("driveConfig missing")
        sys.exit(1)

    indexed = {(e["category"], e["key"], e["value"]) for e in config_json}

    for line in taped_config.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(None, 3)
        if len(parts) < 3:
            continue
        cat, key, val = parts[0], parts[1], parts[2]
        # Because our config files are badly structured, some options end up differently in the catalogue
        # For now just skip them
        if key in KEY_SKIP_LIST:
            continue
        if (cat, key, val) not in indexed:
            print(f"Missing: {cat} {key} {val}")


if __name__ == "__main__":
    main()
