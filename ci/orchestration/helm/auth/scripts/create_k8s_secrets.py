# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import os
import base64
import json
import ssl
import re
import sys
import argparse
import urllib.request


def k8s_create_secret(namespace, secret_name, filename, filepath, host):
    token_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    ca_cert_path = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

    with open(filepath, "rb") as f:
        filedata = base64.b64encode(f.read()).decode()

    payload = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": secret_name, "namespace": namespace},
        "type": "Opaque",
        "data": {filename: filedata},
    }

    try:
        with open(token_path, "r") as f:
            token = f.read().strip()

        context = ssl.create_default_context(cafile=ca_cert_path)
        url = f"{host}/api/v1/namespaces/{namespace}/secrets"

        req = urllib.request.Request(url, data=json.dumps(payload).encode(), method="POST")
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Content-Type", "application/json")

        with urllib.request.urlopen(req, context=context):
            print(f"Created secret: {secret_name}")
    except Exception as e:
        print(f"Error creating {secret_name}: {e}")


def is_valid_name(name):
    return len(name) <= 63 and re.fullmatch(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?", name)


def main():
    parser = argparse.ArgumentParser(description="Create a K8s secret for every file in the provided directory.")
    parser.add_argument("--namespace", "-n", required=True, help="Kubernetes namespace to create the secrets in")
    parser.add_argument("--dir", "-d", required=True, help="Directory containing secret files")

    args = parser.parse_args()

    # K8s Service Host/Port injected by K8s itself
    k_host = os.getenv("KUBERNETES_SERVICE_HOST")
    k_port = os.getenv("KUBERNETES_SERVICE_PORT")

    if not k_host or not k_port:
        print("Error: KUBERNETES_SERVICE_HOST/PORT environment variables not set.")
        sys.exit(1)

    full_host = f"https://{k_host}:{k_port}"

    if not os.path.exists(args.dir):
        print(f"Error: Directory not found: {args.dir}")
        sys.exit(1)

    for fname in os.listdir(args.dir):
        fpath = os.path.join(args.dir, fname)
        if not os.path.isfile(fpath):
            continue

        s_name = fname.replace(".", "-").lower()

        if is_valid_name(s_name):
            k8s_create_secret(args.namespace, s_name, fname, fpath, full_host)
        else:
            print(f"Error: Failed to create secret for '{fname}'. Invalid name: '{s_name}'")
            exit(1)


if __name__ == "__main__":
    main()
