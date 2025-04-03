from fastapi import APIRouter

router = APIRouter(prefix="", tags=[], dependencies=[])


@router.get("/")
async def root():
    return {"message": "Welcome to the CTA REST API"}


@router.get("/status")
def status_check():
    return {"status": "ok"}
