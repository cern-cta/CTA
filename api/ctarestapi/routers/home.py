from fastapi import APIRouter

router = APIRouter(prefix="", tags=[], dependencies=[])

@router.get("/")
async def root():
    return {"message": "Welcome to the CTA REST API"}


@router.get("/health/ready")
def healthiness_probe():
    # We can eventually check if the catalogue is reachable here
    # Then we can return the catalogue version and API version
    return {"status": "ok"}
