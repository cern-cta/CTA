from fastapi import APIRouter, Depends, Query, HTTPException
from ctarestapi.dependencies import Catalogue, get_catalogue
from pydantic import BaseModel, StringConstraints, model_validator
from typing import Annotated, Optional
from enum import Enum

router = APIRouter(prefix="/drives", tags=["drives"], dependencies=[])


@router.get("/")
async def get_drives(
    limit: int = Query(100),
    offset: int = Query(0),
    catalogue: Catalogue = Depends(get_catalogue),
):
    drives = catalogue.drives.get_all_drives(limit=limit, offset=offset)
    return drives


@router.get("/{drive_name}")
async def get_drive(drive_name: str, catalogue: Catalogue = Depends(get_catalogue)):
    drive = catalogue.drives.get_drive(drive_name=drive_name)
    if not drive:
        raise HTTPException(status_code=404, detail="Drive not found")
    return drive


class DesiredDriveState(str, Enum):
    up = "up"
    down = "down"


class DriveStateUpdate(BaseModel):
    desired_state: DesiredDriveState
    reason: Optional[str] = ""

    @model_validator(mode="after")
    def validate_reason_required(self):
        if self.desired_state == DesiredDriveState.down and not self.reason:
            raise ValueError("reason is required when desired_state is 'down'")
        return self


@router.put("/{drive_name}/state")
async def update_drive_state(
    drive_name: str,
    state_update: DriveStateUpdate,
    force: bool = Query(False),
    catalogue: Catalogue = Depends(get_catalogue),
):
    if state_update.desired_state == DesiredDriveState.up:
        success = catalogue.drives.set_drive_up(drive_name, state_update.reason)
    else:
        success = catalogue.drives.set_drive_down(
            drive_name, state_update.reason, force
        )

    if not success:
        raise HTTPException(status_code=404, detail="Drive not found")
    return {"status": "ok"}


class DriveCommentUpdate(BaseModel):
    comment: Annotated[str, StringConstraints(min_length=1, max_length=1000)]


@router.put("/{drive_name}/comment")
async def update_drive_comment(
    drive_name: str,
    update: DriveCommentUpdate,
    catalogue: Catalogue = Depends(get_catalogue),
):
    success = catalogue.drives.update_drive_comment(drive_name, update.comment)
    if not success:
        raise HTTPException(status_code=404, detail="Drive not found")
    return {"status": "ok"}


@router.delete("/{drive_name}")
async def delete_drive(
    drive_name: str,
    force: bool = Query(False),
    catalogue: Catalogue = Depends(get_catalogue),
):
    raise HTTPException(status_code=501, detail="Not implemented yet.")
