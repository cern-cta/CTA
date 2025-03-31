from fastapi import FastAPI
from .routers import drives

app = FastAPI(
    title="CTA REST API",
    description="A REST API for the CTA storage software",
    version="0.0.1",
     contact={
        "name": "CTA Community Forum",
        "url": "https://cta-community.web.cern.ch"
    },
    license_info={
        "name": "GNU GPLv3",
        "url": "https://gitlab.cern.ch/cta/CTA/-/blob/main/COPYING",
    },
)

app.include_router(drives.router)

@app.get("/")
async def root():
    return {"message": "Welcome to the CTA REST API"}


@app.get("/health/ready")
def readiness_probe():
    return {"status": "ok"}
