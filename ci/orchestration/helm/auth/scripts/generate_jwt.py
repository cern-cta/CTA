# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import json
import argparse
import re
from pathlib import Path

import jwt
from cryptography.hazmat.primitives import serialization


def sanitize_filename(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]", "_", s)


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
    parser = argparse.ArgumentParser(description="Generate a CI JWT file in the --output-dir directory.")
    parser.add_argument(
        "--claims",
        required=True,
        help="JWT payload claims as a JSON object.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="/tmp",
        help="Directory to put the generated file in",
    )
    parser.add_argument(
        "--key",
        required=True,
        help="Path to private key",
    )
    parser.add_argument(
        "--jwt-filename",
        help="Name of the generated JWT file.",
    )
    parser.add_argument(
        "--key-id",
        default="rsa1",
        help="Key ID to use in the JWT header.",
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

    token = generate_jwt(
        key,
        args.key_id,
        claims,
    )

    safe_sub = sanitize_filename(str(claims.get("sub", "token")))
    jwt_path = Path(args.output_dir) / (args.jwt_filename or f"{safe_sub}.jwt")

    with open(jwt_path, "w") as f:
        f.write(token)

    print(f"Generated {jwt_path}")


if __name__ == "__main__":
    main()
