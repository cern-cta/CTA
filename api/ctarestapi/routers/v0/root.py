from fastapi import APIRouter, Response, Depends
from ctarestapi.version import __version__
from ctarestapi.dependencies import Catalogue, get_catalogue
from ctarestapi.internal.catalogue import CatalogueStatus

router = APIRouter(prefix="", tags=[], dependencies=[])


@router.get("/")
async def root():
    return {"message": "Welcome to the CTA REST API"}


@router.get("/health")
def status_check():
    return Response(status_code=200)


@router.get("/status")
def get_status(catalogue: Catalogue = Depends(get_catalogue)):
    catalogue_status: CatalogueStatus = catalogue.get_status()

    api_status = "ok"
    components_status_list = [catalogue_status.status]
    if "unavailable" in components_status_list:
        api_status = "degraded"
    if all(v == "unavailable" for v in components_status_list):
        api_status = "unavailable"

    return {
        "status": api_status,
        "version": __version__,
        "components": {"catalogue": catalogue_status.model_dump()},
    }
