import json, jwt, datetime
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from jwt.utils import base64url_encode

with open("/data/public.pem", "rb") as f:
    pub = serialization.load_pem_public_key(f.read(), backend=default_backend())
    n = base64url_encode(pub.public_numbers().n.to_bytes(256, 'big')).decode()
    e = base64url_encode(pub.public_numbers().e.to_bytes(3, 'big')).decode()

jwks = {
    "keys": [{
        "kty": "RSA",
        "alg": "RS256",
        "use": "sig",
        "kid": "ci-key",
        "n": n,
        "e": e
    }]
}
with open("/data/jwks.json", "w") as f:
    json.dump(jwks, f)

with open("/data/private.key", "rb") as f:
    private_key = f.read()

token = jwt.encode(
    {
        "username": "ci-user",
        "exp": datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=240),
    },
    key=private_key,
    algorithm="RS256",
    headers={"kid": "ci-key"}
)
with open("/data/token.jwt", "w") as f:
    f.write(token)
