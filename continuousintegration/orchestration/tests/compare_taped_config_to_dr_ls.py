#!/usr/bin/env python3
import argparse
import subprocess
import json
import sys

CTA_TPSRV_POD = "cta-tpsrv01-0"
CTA_CLI_POD = "cta-cli-0"

KEY_SKIP_LIST = ["MountCriteria"]

def run(cmd):
    p = subprocess.run(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    if p.returncode != 0:
        raise RuntimeError(f"cmd failed: {cmd}\n{p.stderr}")
    return p.stdout


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--namespace", required=True)
    args = ap.parse_args()

    ns = args.namespace

    taped_config = run(
        f"kubectl -n {ns} exec {CTA_TPSRV_POD} -c cta-taped-0 -- "
        "bash -c 'cat /etc/cta/cta-taped*.conf'"
    )

    drive_name = run(
        f"kubectl -n {ns} exec {CTA_TPSRV_POD} -c cta-taped-0 -- printenv DRIVE_NAME"
    ).strip()

    drive_json = run(
        f"kubectl -n {ns} exec {CTA_CLI_POD} -c cta-cli -- "
        "cta-admin --json dr ls"
    )

    entries = [
        e
        for e in json.loads(drive_json)
        if e.get("driveName") == drive_name
    ]

    if not entries:
        print("Drive not found")
        sys.exit(1)

    config_json = entries[0].get("driveConfig")
    if config_json is None:
        print("driveConfig missing")
        sys.exit(1)

    indexed = {
        (e["category"], e["key"], e["value"])
        for e in config_json
    }

    for line in taped_config.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(None, 3)
        if len(parts) < 3:
            continue
        cat, key, val = parts[0], parts[1], parts[2]
        # Because our config files are crap, these options (for various reasons) end up differently in the catalogue
        # For now just skip them
        if key in KEY_SKIP_LIST:
            continue
        if (cat, key, val) not in indexed:
            print(f"Missing: {cat} {key} {val}")


if __name__ == "__main__":
    main()
