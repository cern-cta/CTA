import os
from fastapi import FastAPI
from fastapi.middleware import Middleware
from .routers import drives
from .routers import home
from .middleware import JWTMiddleware


def create_app() -> FastAPI:
    jwks_endpoint = os.environ.get("JWKS_ENDPOINT")
    if not jwks_endpoint:
        raise RuntimeError("Missing required environment variable: JWKS_ENDPOINT")
    jwks_cache_expiry = int(os.environ.get("JWKS_CACHE_EXPIRY", 300))

    app = FastAPI(
        title="CTA REST API",
        description="A REST API for the CTA storage software",
        version="0.0.1",
        contact={"name": "CTA Community Forum", "url": "https://cta-community.web.cern.ch"},
        license_info={
            "name": "GNU GPLv3",
            "url": "https://gitlab.cern.ch/cta/CTA/-/blob/main/COPYING",
        }
    )
    app.add_middleware(JWTMiddleware, jwks_endpoint=jwks_endpoint, jwks_cache_expiry=jwks_cache_expiry)

    app.include_router(home.router)
    app.include_router(drives.router)
    return app


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(create_app(), host="0.0.0.0", port=80)
