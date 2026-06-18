# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import json
import time
import base64
import hashlib
import argparse
import re
from pathlib import Path

import jwt
from cryptography import x509
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey
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


def generate_jwk_from_cert(cert_path):
    with open(cert_path, "rb") as f:
        cert_data = f.read()

    if b"BEGIN CERTIFICATE" in cert_data:
        cert_obj = x509.load_pem_x509_certificate(cert_data)
    else:
        cert_obj = x509.load_der_x509_certificate(cert_data)

    public_key = cert_obj.public_key()
    if not isinstance(public_key, RSAPublicKey):
        raise ValueError("Certificate public key must be RSA")

    pub_numbers = public_key.public_numbers()

    def int_to_base64url(n: int) -> str:
        byte_length = (n.bit_length() + 7) // 8
        return base64.urlsafe_b64encode(n.to_bytes(byte_length, "big")).rstrip(b"=").decode()

    n_b64 = int_to_base64url(pub_numbers.n)
    e_b64 = int_to_base64url(pub_numbers.e)

    # RFC 7638 thumbprint: SHA-256 over sorted, minimal JSON of {e, kty, n}
    thumbprint_input = json.dumps(
        {"e": e_b64, "kty": "RSA", "n": n_b64},
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    thumbprint = hashlib.sha256(thumbprint_input).digest()
    kid = base64.urlsafe_b64encode(thumbprint).rstrip(b"=").decode()

    jwk = {
        "kty": "RSA",
        "alg": "RS256",
        "use": "sig",
        "kid": kid,
        "n": n_b64,
        "e": e_b64,
        "x5c": [load_cert_x5c(cert_path)],
    }

    return jwk


def generate_jwt(private_key, kid: str, sub: str, lifetime_sec: int):
    """Generates a JWT with all required claims the CTA frontend needs to verify it"""
    now = int(time.time())

    payload = {
        "iat": now,
        "exp": now + lifetime_sec,
        "sub": sub,
        "typ": "Bearer",
    }

    token = jwt.encode(
        payload,
        private_key,
        algorithm="RS256",
        headers={"kid": kid, "typ": "JWT"},
    )

    return token


def main():
    parser = argparse.ArgumentParser(
        description="Generate CI files containing a JWKS and one JWT for each --sub passed. Files are put in the --output-dir directory."
    )
    parser.add_argument(
        "--sub",
        action="append",
        required=True,
        help="Subject claim (repeatable). Each --sub argument will generate a separate JWT with the name of the sub.",
    )
    parser.add_argument(
        "--lifetime",
        type=int,
        default=60 * 60 * 24 * 90,
        help="Token lifetime in seconds",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="/tmp",
        help="Directory to put the generated files in",
    )
    parser.add_argument("--cert", required=True, help="Path to server certificate")
    parser.add_argument("--key", required=True, help="Path to private key")
    parser.add_argument("--jwks", help="Filename for the generated JWKS file")

    args = parser.parse_args()

    with open(args.key, "rb") as f:
        key = serialization.load_pem_private_key(f.read(), password=None)
    jwk = generate_jwk_from_cert(args.cert)

    if args.jwks is not None:
        print(f"Adding key from cert {args.cert} to JWKS file {args.jwks}")
        jwks_path = Path(args.output_dir) / args.jwks
        if jwks_path.exists():
            print("JWKS file already exists, appending key")
            with open(jwks_path) as f:
                jwks = json.load(f)
            jwks["keys"].append(jwk)
        else:
            jwks = {"keys": [jwk]}

        with open(jwks_path, "w") as f:
            json.dump(jwks, f, indent=2)

        print(f"Generated {jwks_path}")

    # Generate one file per sub
    for sub in args.sub:
        token = generate_jwt(key, jwk["kid"], sub, args.lifetime)

        safe_sub = sanitize_filename(sub)
        jwt_path = Path(args.output_dir) / f"{safe_sub}.jwt"

        with open(jwt_path, "w") as f:
            f.write(token)

        print(f"Generated {jwt_path}")


if __name__ == "__main__":
    main()
