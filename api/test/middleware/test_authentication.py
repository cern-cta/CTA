import jwt
import json
import pytest
import datetime
from fastapi.testclient import TestClient
from jwt.utils import base64url_encode
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from unittest.mock import patch, MagicMock, Mock
from ctarestapi.main import create_app
from ctarestapi.dependencies import get_catalogue

KID = "test-key"
ALGORITHM = "RS256"


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


def test_valid_jwt_returns_200(client_with_auth):
    exp = datetime.datetime.now() + datetime.timedelta(minutes=5)
    valid_token = client_with_auth.keyPair.generate_valid_jwt(exp=exp)

    response = client_with_auth.get("/protected", headers={"Authorization": f"Bearer {valid_token}"})
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_expired_token_returns_401(client_with_auth):
    exp = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=5)
    expired_token = client_with_auth.keyPair.generate_valid_jwt(exp=exp)

    response = client_with_auth.get("/protected", headers={"Authorization": f"Bearer {expired_token}"})
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid or expired token"


def test_no_expiration_present_returns_401(client_with_auth):
    kp = client_with_auth.keyPair
    invalid_token = kp.generate_jwt(
        headers={"kid": kp.get_kid()}, body={}, pem=kp.get_pem(), algorithm=kp.get_algorithm()
    )

    response = client_with_auth.get("/protected", headers={"Authorization": f"Bearer {invalid_token}"})
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid or expired token"


def test_algo_none_returns_401(client_with_auth):
    exp = datetime.datetime.now() + datetime.timedelta(minutes=5)
    kp = client_with_auth.keyPair
    invalid_token = kp.generate_jwt(headers={"kid": kp.get_kid()}, body={"exp": exp}, pem="", algorithm=None)

    response = client_with_auth.get("/protected", headers={"Authorization": f"Bearer {invalid_token}"})
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid or expired token"


def test_incorrect_signature_returns_401(client_with_auth):
    exp = datetime.datetime.now() + datetime.timedelta(minutes=5)
    kp = client_with_auth.keyPair
    token = kp.generate_jwt(
        headers={"kid": kp.get_kid()}, body={"exp": exp}, pem=kp.get_pem(), algorithm=kp.get_algorithm()
    )
    parts = token.split(".")
    assert len(parts) == 3

    # Tamper the signature (part 2)
    parts[2] = "invalidstuff"
    invalid_token = ".".join(parts)

    print(token)
    print(invalid_token)

    response = client_with_auth.get("/protected", headers={"Authorization": f"Bearer {invalid_token}"})
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid or expired token"


def test_nonexisting_kid_returns_401(client_with_auth):
    exp = datetime.datetime.now() + datetime.timedelta(minutes=5)
    kp = client_with_auth.keyPair
    invalid_token = kp.generate_jwt(
        headers={"kid": "idontexist"}, body={"exp": exp}, pem=kp.get_pem(), algorithm=kp.get_algorithm()
    )

    response = client_with_auth.get("/protected", headers={"Authorization": f"Bearer {invalid_token}"})
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid or expired token"


def test_missing_kid_returns_401(client_with_auth):
    exp = datetime.datetime.now() + datetime.timedelta(minutes=5)
    kp = client_with_auth.keyPair
    invalid_token = kp.generate_jwt(headers={}, body={"exp": exp}, pem=kp.get_pem(), algorithm=kp.get_algorithm())

    response = client_with_auth.get("/protected", headers={"Authorization": f"Bearer {invalid_token}"})
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid or expired token"


def test_missing_token_returns_401(client_with_auth):
    response = client_with_auth.get("/protected")
    assert response.status_code == 401
    assert response.json()["detail"] == "Missing or invalid Authorization header"
