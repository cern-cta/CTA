# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import argparse
import base64
import json
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import serialization


def load_cert_x5c(cert_path: str) -> str:
    with open(cert_path, "rb") as f:
        cert = f.read()

    if b"BEGIN CERTIFICATE" in cert:
        cert_obj = x509.load_pem_x509_certificate(cert)
        cert = cert_obj.public_bytes(serialization.Encoding.DER)

    return base64.b64encode(cert).decode("ascii")


def base64url_uint(value: int) -> str:
    value_bytes = value.to_bytes((value.bit_length() + 7) // 8, byteorder="big")
    return base64.urlsafe_b64encode(value_bytes).rstrip(b"=").decode("ascii")


def generate_jwk(private_key, cert_path: str, kid: str):
    public_numbers = private_key.public_key().public_numbers()

    return {
        "kty": "RSA",
        "alg": "RS256",
        "use": "sig",
        "x5c": [load_cert_x5c(cert_path)],
        "e": base64url_uint(public_numbers.e),
        "kid": kid,
        "n": base64url_uint(public_numbers.n),
    }


def main():
    parser = argparse.ArgumentParser(description="Generate a CI JWKS file in the --output-dir directory.")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="/tmp",
        help="Directory to put the generated file in",
    )
    parser.add_argument(
        "--cert",
        required=True,
        help="Path to server certificate",
    )
    parser.add_argument(
        "--key",
        required=True,
        help="Path to private key",
    )
    parser.add_argument(
        "--jwks-filename",
        default="jwks.json",
        help="Name of the generated JWKS file",
    )
    parser.add_argument(
        "--key-id",
        default="rsa1",
        help="Key ID to use in the JWKS.",
    )

    args = parser.parse_args()

    with open(args.key, "rb") as f:
        key = serialization.load_pem_private_key(f.read(), password=None)

    jwks = {"keys": [generate_jwk(key, args.cert, args.key_id)]}
    jwks_path = Path(args.output_dir) / args.jwks_filename

    with open(jwks_path, "w") as f:
        json.dump(jwks, f, indent=2)

    print(f"Generated {jwks_path}")


if __name__ == "__main__":
    main()
