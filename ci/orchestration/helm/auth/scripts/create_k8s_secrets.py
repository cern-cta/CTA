# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import argparse
import base64
import json
import os
import re
import ssl
import sys
import urllib.request
from pathlib import Path


def k8s_create_secret(namespace: str, secret_name: str, filepath: Path, host: str) -> None:
    token_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
    ca_cert_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")

    with filepath.open("rb") as f:
        filedata = base64.b64encode(f.read()).decode()

    payload = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": secret_name, "namespace": namespace},
        "type": "Opaque",
        "data": {filepath.name: filedata},
    }

    try:
        with token_path.open() as f:
            token = f.read().strip()

        context = ssl.create_default_context()
        context.load_verify_locations(cafile=str(ca_cert_path))
        url = f"{host}/api/v1/namespaces/{namespace}/secrets"

        req = urllib.request.Request(url, data=json.dumps(payload).encode(), method="POST")
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Content-Type", "application/json")

        with urllib.request.urlopen(req, context=context) as response:
            if response.status in (200, 201):
                print(f"Successfully created secret: {secret_name}")
            else:
                print(f"Unexpected status code {response.status} for {secret_name}")
    except Exception as e:
        print(f"Error creating {secret_name}: {e}")


def is_valid_name(name: str) -> bool:
    return bool(len(name) <= 63 and re.fullmatch(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?", name))


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a K8s secret for every file in the provided directory.")
    parser.add_argument("--namespace", "-n", required=True, help="Kubernetes namespace to create the secrets in")
    parser.add_argument("--dir", "-d", required=True, type=Path, help="Directory containing secret files")

    args = parser.parse_args()

    # K8s Service Host/Port injected by K8s itself
    k_host = os.getenv("KUBERNETES_SERVICE_HOST")
    k_port = os.getenv("KUBERNETES_SERVICE_PORT")

    if not k_host or not k_port:
        print("Error: KUBERNETES_SERVICE_HOST/PORT environment variables not set.")
        sys.exit(1)

    full_host = f"https://{k_host}:{k_port}"

    if not args.dir.exists():
        print(f"Error: Directory not found: {args.dir}")
        sys.exit(1)

    for fpath in args.dir.iterdir():
        if not fpath.is_file():
            continue

        s_name = fpath.name.replace(".", "-").lower()

        if is_valid_name(s_name):
            k8s_create_secret(args.namespace, s_name, fpath, full_host)
        else:
            print(f"Error: Failed to create secret for '{fpath.name}'. Invalid name: '{s_name}'")
            exit(1)


if __name__ == "__main__":
    main()
