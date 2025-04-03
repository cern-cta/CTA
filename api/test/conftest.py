import jwt
import json
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock, Mock
from ctarestapi.server import create_app
from ctarestapi.dependencies import get_catalogue
from fastapi.testclient import TestClient
from jwt.utils import base64url_encode
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("JWKS_ENDPOINT", "https://fake-jwks.local/.well-known/jwks.json")
    mock_catalogue = Mock()

    # We ignore all middleware to ensure we can test the endpoints directly
    # There will be separate tests specifically for the middleware
    with patch.object(FastAPI, "add_middleware", lambda *args, **kwargs: None):
        app = create_app()
        app.dependency_overrides[get_catalogue] = lambda: mock_catalogue

        client = TestClient(app)
        client.mock_catalogue = mock_catalogue  # attach for access in tests
        yield client

        app.dependency_overrides.clear()


# For testing purposes
class KeyPair:

    def __init__(self, algorithm: str, kid: str):
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        public_key = private_key.public_key()

        # Private key encoded in PEM format
        self._pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        self._algorithm = algorithm
        self._kid = kid

        pub_numbers = public_key.public_numbers()
        n = base64url_encode(pub_numbers.n.to_bytes((pub_numbers.n.bit_length() + 7) // 8, "big")).decode()
        e = base64url_encode(pub_numbers.e.to_bytes((pub_numbers.e.bit_length() + 7) // 8, "big")).decode()

        self._jwks = {
            "keys": [
                {
                    "kty": "RSA",
                    "alg": algorithm,
                    "use": "sig",
                    "kid": kid,
                    "n": n,
                    "e": e,
                }
            ]
        }

    def get_jwks(self):
        return self._jwks

    def get_pem(self):
        return self._pem

    def get_algorithm(self):
        return self._algorithm

    def get_kid(self):
        return self._kid

    def generate_valid_jwt(self, exp: int):
        return jwt.encode(
            {"exp": exp},
            self._pem,
            algorithm=self._algorithm,
            headers={"kid": self._kid},
        )

    # This is generic on purpose to generate all kinds of invalid tokens
    def generate_jwt(self, headers: dict, body: dict, pem: bytes, algorithm: str):
        return jwt.encode(
            body,
            pem,
            algorithm=algorithm,
            headers=headers,
        )


@pytest.fixture
def client_with_auth(monkeypatch):
    monkeypatch.setenv("JWKS_ENDPOINT", "https://jwks.test/.well-known/jwks.json")
    mock_catalogue = Mock()

    keyPair = KeyPair(kid="unit-test-key", algorithm="RS256")
    jwks_json = json.dumps(keyPair.get_jwks()).encode()

    # Mock urllib.request.urlopen
    mock_response = MagicMock()
    mock_response.read.return_value = jwks_json
    mock_response.__enter__.return_value = mock_response

    with patch("urllib.request.urlopen", return_value=mock_response):
        app = create_app()
        app.dependency_overrides[get_catalogue] = lambda: mock_catalogue

        # Define a plain route we can test the JWT middleware against
        @app.get("/protected")
        async def protected():
            return {"status": "ok"}

        client = TestClient(app)
        client.mock_catalogue = mock_catalogue
        client.keyPair = keyPair
        yield client

        app.dependency_overrides.clear()
