import jwt
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send
from jwt import PyJWKClient, InvalidTokenError, ExpiredSignatureError, MissingRequiredClaimError, PyJWKClientError

# See https://www.rfc-editor.org/rfc/rfc6750 for the JWT location in the request

class JWTMiddleware():

    _app: ASGIApp
    _jwks_client: PyJWKClient
    _allowed_algorithms: set[str]

    def __init__(self, app, allowed_algorithms: set[str], jwks_endpoint: str, jwks_cache_expiry: int):
        self._app = app
        self._allowed_algorithms = allowed_algorithms
        self._jwks_client = PyJWKClient(jwks_endpoint, cache_keys=True, cache_jwk_set=True, lifespan=jwks_cache_expiry)

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        request = Request(scope, receive=receive)

        auth = request.headers.get("Authorization")
        if not auth or not auth.startswith("Bearer "):
            response = JSONResponse(
                status_code=401,
                content={"detail": "Missing or invalid Authorization header"},
            )
            await response(scope, receive, send)
            return

        token = auth.removeprefix("Bearer ").strip()

        try:
            signing_jwk = self._jwks_client.get_signing_key_from_jwt(token)
            alg = signing_jwk._jwk_data.get("alg", "RS256")
            if alg not in self._allowed_algorithms:
                raise InvalidTokenError(f"Unsupported algorithm: {alg}")

            jwt.decode(token, signing_jwk.key, algorithms=[alg], options={"require": ["exp"]})

        except (PyJWKClientError, InvalidTokenError, ExpiredSignatureError, MissingRequiredClaimError, Exception):
            response = JSONResponse(
                status_code=401,
                content={"detail": "Invalid or expired token"},
            )
            await response(scope, receive, send)
            return

        await self._app(scope, receive, send)
