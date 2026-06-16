# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import json
import base64
import argparse
import re
from pathlib import Path

import jwt
from cryptography import x509
from cryptography.hazmat.primitives import serialization


def sanitize_filename(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]", "_", s)


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


def generate_jwt(
    private_key,
    kid: str,
    claims: dict,
):
    """Generates a JWT with the given claims."""
    token = jwt.encode(
        claims,
        private_key,
        algorithm="RS256",
        headers={"kid": kid, "typ": "JWT"},
    )

    return token


def main():
    parser = argparse.ArgumentParser(
        description="Generate CI files containing a JWKS and one JWT. Files are put in the --output-dir directory."
    )
    parser.add_argument(
        "--claims",
        required=True,
        help="JWT payload claims as a JSON object.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="/tmp",
        help="Directory to put the generated files in",
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
        "--jwt-filename",
        help="Name of the generated JWT file.",
    )
    parser.add_argument(
        "--key-id",
        default="rsa1",
        help="Key ID to use in the JWKS and JWT header.",
    )

    args = parser.parse_args()

    try:
        claims = json.loads(args.claims)
    except json.JSONDecodeError as err:
        parser.error(f"--claims must be a valid JSON: {err}")

    if not isinstance(claims, dict):
        parser.error("--claims must be a JSON object")

    with open(args.key, "rb") as f:
        key = serialization.load_pem_private_key(f.read(), password=None)

    jwk = generate_jwk(key, args.cert, args.key_id)

    # Save JWKS
    jwks = {"keys": [jwk]}
    jwks_path = Path(args.output_dir) / args.jwks_filename
    with open(jwks_path, "w") as f:
        json.dump(jwks, f, indent=2)

    print(f"Generated {jwks_path}")

    token = generate_jwt(
        key,
        jwk["kid"],
        claims,
    )

    safe_sub = sanitize_filename(str(claims.get("sub", "token")))
    jwt_path = Path(args.output_dir) / (args.jwt_filename or f"{safe_sub}.jwt")

    with open(jwt_path, "w") as f:
        f.write(token)

    print(f"Generated {jwt_path}")


if __name__ == "__main__":
    main()
