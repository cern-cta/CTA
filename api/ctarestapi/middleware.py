import jwt
from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from jwt import PyJWKClient, InvalidTokenError, ExpiredSignatureError, MissingRequiredClaimError

# See https://www.rfc-editor.org/rfc/rfc6750 for the JWT location in the request

ALLOWED_ALGORITHMS = {"RS256", "ES256"}

class JWTMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, jwks_endpoint: str, jwks_cache_expiry: int):
        super().__init__(app)
        self._jwks_client = PyJWKClient(jwks_endpoint)
        self._jwks_cache_expiry = jwks_cache_expiry
        self._jwks_keys = []

    async def dispatch(self, request: Request, call_next):
        auth = request.headers.get("Authorization")
        if not auth or not auth.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")

        token = auth.removeprefix("Bearer ").strip()

        try:
            signing_jwk = self._jwks_client.get_signing_key_from_jwt(token)
            alg = signing_jwk._jwk_data.get("alg", "RS256")  # fallback to RS256
            if alg not in ALLOWED_ALGORITHMS:
                raise HTTPException(status_code=401, detail=f"Unsupported algorithm: {alg}")

            # Expiration date "exp" is checked by decode
            decoded = jwt.decode(token, signing_jwk.key, algorithms=[alg], options={"require": ["exp"]})

            # We can add more info from the JWT to the request object here
            # This will allow for RBAC in the future.


        except ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Token has expired")
        except MissingRequiredClaimError:
            raise HTTPException(status_code=401, detail="Token missing required claim: exp")
        except InvalidTokenError as e:
            raise HTTPException(status_code=401, detail=f"Invalid token: {str(e)}")

        return await call_next(request)
