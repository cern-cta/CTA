import os
import sys
import logging
from fastapi import FastAPI
from ctarestapi.routers import drives
from ctarestapi.routers import home
from ctarestapi.middleware.jwt_middleware import JWTMiddleware

# Set the logging level from the env variable LOGLEVEL
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())


def create_app() -> FastAPI:
    jwks_endpoint = os.environ.get("JWKS_ENDPOINT")
    if not jwks_endpoint:
        raise RuntimeError("Missing required environment variable: JWKS_ENDPOINT")
    jwks_cache_expiry = int(os.environ.get("JWKS_CACHE_EXPIRY", 300))

    app = FastAPI(
        title="CTA REST API",
        description="A REST API for the CTA storage software",
        version="0.0.1",
        contact={
            "name": "CTA Community Forum",
            "url": "https://cta-community.web.cern.ch",
        },
        license_info={
            "name": "GNU GPLv3",
            "url": "https://gitlab.cern.ch/cta/CTA/-/blob/main/COPYING",
        },
    )

    allowed_algorithms = {"RS256", "ES256"}
    app.add_middleware(
        JWTMiddleware,
        allowed_algorithms=allowed_algorithms,
        jwks_endpoint=jwks_endpoint,
        jwks_cache_expiry=jwks_cache_expiry,
        unauthenticated_routes={"/status"},
    )

    app.include_router(home.router)
    app.include_router(drives.router)
    return app


if __name__ == "__main__":
    import uvicorn

    try:
        uvicorn.run(create_app(), host="0.0.0.0", port=80)
    except Exception as error:
        logging.error(f"FATAL: {error}")
        sys.exit(1)
