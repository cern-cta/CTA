# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import base64
import hashlib
import json
import re
import time
import uuid
from dataclasses import InitVar, asdict, dataclass, field
from pathlib import Path
from tempfile import gettempdir
from typing import Literal, Optional

import click
from rich import print

from ctaops.cli.click_extension.aliases import ClickAliasedGroup
from ctaops.cli.context import AppContext
from ctaops.cli.display.printers import print_dataclasses

JWKS_CONTENT = dict[Literal["keys"], list[dict[str, str]]]


@dataclass
class JWK:
    kty: Literal["RSA"]
    alg: Literal["RS256"]
    use: Literal["sig"]
    kid: str
    n: str
    e: str


@dataclass
class JWTClaim:
    sub: str
    iss: str
    iat: int
    gen: int
    lifetime_sec: InitVar[int]
    exp: int = field(init=False)
    jti: Optional[str] = None
    aud: Optional[str] = None
    typ: Literal["Bearer"] = "Bearer"

    def __post_init__(self, lifetime_sec) -> None:
        self.exp = self.iat + lifetime_sec

        if self.jti is None:
            self.jti = str(uuid.uuid4())

        if self.typ != "Bearer":
            raise ValueError(f"Token {self.jti} is of type {self.typ}, only type Bearer is accepted")


def generate_jwk_from_public_key(
    public_key_path: Path,
) -> JWK:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey

    public_key = serialization.load_pem_public_key(
        public_key_path.read_bytes(),
    )
    if not isinstance(public_key, RSAPublicKey):
        raise TypeError("Certificate public key must be RSA")

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

    return JWK(kty="RSA", alg="RS256", use="sig", kid=kid, n=n_b64, e=e_b64)


@click.group(cls=ClickAliasedGroup, aliases=["to"])
def token():
    """Manage JWT"""


@token.command(aliases=["create"])
@click.argument(
    "subject",
    nargs=-1,
    required=True,
)
@click.option(
    "--generation",
    "--gen",
    type=int,
    help="Generation (version) of the token. Can be used for mass-revocation of tokens by the server.",
    required=True,
)
@click.option(
    "--pub", required=True, type=click.Path(exists=True, readable=True, path_type=Path), help="Path to public key"
)
@click.option(
    "--key", required=True, type=click.Path(exists=True, readable=True, path_type=Path), help="Path to private key"
)
@click.option("--password", help="Private key password", type=bytes)
@click.option("--issuer", default="ctaops-cli", help="Issuer claim to set in the generated JWT")
@click.option("--audience", "--aud", help="Audience claim to set in the generated JWT")
@click.option("--lifetime", type=int, default=60 * 60 * 24 * 90, help="Token lifetime in seconds", show_default=True)
@click.option("--jti", help="Set a custom JTI (useful for testing)")
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=Path(gettempdir()),
    help="Directory to put the generated files in",
)
@click.pass_obj
def generate(
    obj: AppContext,
    subject: tuple[str, ...],
    generation: int,
    pub: Path,
    key: Path,
    password: Optional[bytes],
    issuer: str,
    audience: Optional[str],
    lifetime: int,
    jti: Optional[str],
    output_dir: Path,
):
    """Generate a new JWT token
    <subject> claim (repeatable). Each <subject> argument will generate a separate JWT with the name of the subject",
    """

    import jwt
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

    def generate_jwt(
        private_key: RSAPrivateKey,
        kid: str,
        jwt_claim: JWTClaim,
    ) -> str:
        """Generate a JWT with all claims required by the CTA frontend."""
        payload = {
            "sub": jwt_claim.sub,
            "jti": jwt_claim.jti,
            "iat": jwt_claim.iat,
            "gen": jwt_claim.gen,
            "exp": jwt_claim.exp,
            "typ": jwt_claim.typ,
            "iss": jwt_claim.iss,
        }
        payload |= {"aud": jwt_claim.aud} if jwt_claim.aud is not None else {}

        return jwt.encode(
            payload,
            private_key,
            algorithm="RS256",
            headers={"kid": kid, "typ": "JWT"},
        )

    private_key = serialization.load_pem_private_key(key.read_bytes(), password=password)
    if not isinstance(private_key, RSAPrivateKey):
        raise TypeError("Private key must be RSA")

    jwk = generate_jwk_from_public_key(pub)

    jwt_claims = [
        JWTClaim(
            sub=sub,
            lifetime_sec=lifetime,
            iss=issuer,
            gen=generation,
            iat=int(time.time()),
            aud=audience,
            jti=jti,
        )
        for sub in subject
    ]
    print_dataclasses(jwt_claims, display_config=obj.config.display, title="Token claims")

    for claim in jwt_claims:
        token = generate_jwt(private_key, jwk.kid, claim)
        jwt_path = output_dir / (re.sub(r"[^a-zA-Z0-9._-]", "_", claim.sub) + ".jwt")
        with jwt_path.open("w") as f:
            f.write(token)

        print(f"Token printed at {jwt_path}")


@token.command()
@click.argument(
    "public_key_path",
    required=True,
    nargs=-1,
    type=click.Path(exists=True, readable=True, path_type=Path),
)
@click.option("--output-path", type=click.Path(path_type=Path), default=Path(gettempdir()) / "jwks.json")
@click.pass_obj
def generate_jwks(obj: AppContext, public_key_path: tuple[Path, ...], output_path: Path):
    """Generate jwks file from public key(s)"""

    jwks: list[JWK] = [generate_jwk_from_public_key(key_path) for key_path in public_key_path]
    print_dataclasses(jwks, display_config=obj.config.display)

    if not output_path.exists():
        jwks_json = {"keys": [asdict(jwk) for jwk in jwks]}

    else:
        print("JWKS file already exists, appending key(s)")
        with output_path.open() as f:
            jwks_json = json.load(f)

        jwks_json["keys"].extend([asdict(jwk) for jwk in jwks])

    with output_path.open("w") as f:
        json.dump(jwks_json, f, indent=2)

    print(f"Generated {output_path}")


@token.command(aliases=["show"])
@click.argument("token")
@click.option(
    "--pub", required=True, type=click.Path(exists=True, readable=True, path_type=Path), help="Path to public key"
)
@click.pass_obj
def inspect(obj: AppContext, token: str, pub: Path):
    """Change CTA admin user"""
    import jwt

    try:
        raw_claim = jwt.decode(
            token,
            key=pub.read_bytes(),
            algorithms=["RS256"],
        )

        print_dataclasses(
            JWTClaim(
                sub=raw_claim["sub"],
                iss=raw_claim["iss"],
                iat=raw_claim["iat"],
                jti=raw_claim["jti"],
                gen=raw_claim["gen"],
                lifetime_sec=raw_claim["exp"] - raw_claim["iat"],
                typ=raw_claim["typ"],
                aud=raw_claim.get("aud"),
            ),
            display_config=obj.config.display,
        )
    except (ValueError, jwt.DecodeError) as e:
        raise click.ClickException("Token is invalid") from e
