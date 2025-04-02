from pydantic import BaseModel
from sqlalchemy import text, Engine
from typing import Optional


class DriveConfigEntry(BaseModel):
    category: str
    key: str
    source: str
    value: str


class Drive(BaseModel):
    bytesTransferredInSession: int
    comment: Optional[str]
    ctaVersion: Optional[str]
    currentActivity: Optional[str]
    currentPriority: int
    desiredDriveState: str  # "UP" / "DOWN"
    devFileName: Optional[str]
    diskSystemName: Optional[str]
    driveName: str
    driveStatus: str
    driveStatusSince: int  # seconds since a status timestamp
    filesTransferredInSession: int
    host: str
    logicalLibrary: str
    logicalLibraryDisabled: bool
    mountType: str
    physicalLibrary: Optional[str]
    physicalLibraryDisabled: bool
    rawLibrarySlot: Optional[str]
    reason: Optional[str]
    reservedBytes: Optional[int]
    schedulerBackendName: Optional[str]
    sessionElapsedTime: int
    sessionId: int
    tapepool: Optional[str]
    timeSinceLastUpdate: int
    vid: Optional[str]
    vo: Optional[str]


class DriveQueries:

    def __init__(self, engine: Engine):
        self.engine = engine

    def get_all_drives(self, limit: int = 100, offset: int = 0) -> list[Drive]:
        with self.engine.connect() as conn:
            query = text(
                """
                SELECT
                    DRIVE_STATE.DRIVE_NAME AS DRIVE_NAME,
                    DRIVE_STATE.HOST AS HOST,
                    DRIVE_STATE.LOGICAL_LIBRARY AS LOGICAL_LIBRARY,
                    DRIVE_STATE.SESSION_ID AS SESSION_ID,

                    DRIVE_STATE.BYTES_TRANSFERED_IN_SESSION AS BYTES_TRANSFERED_IN_SESSION,
                    DRIVE_STATE.FILES_TRANSFERED_IN_SESSION AS FILES_TRANSFERED_IN_SESSION,

                    DRIVE_STATE.SESSION_START_TIME AS SESSION_START_TIME,
                    DRIVE_STATE.SESSION_ELAPSED_TIME AS SESSION_ELAPSED_TIME,
                    DRIVE_STATE.MOUNT_START_TIME AS MOUNT_START_TIME,
                    DRIVE_STATE.TRANSFER_START_TIME AS TRANSFER_START_TIME,
                    DRIVE_STATE.UNLOAD_START_TIME AS UNLOAD_START_TIME,
                    DRIVE_STATE.UNMOUNT_START_TIME AS UNMOUNT_START_TIME,
                    DRIVE_STATE.DRAINING_START_TIME AS DRAINING_START_TIME,
                    DRIVE_STATE.DOWN_OR_UP_START_TIME AS DOWN_OR_UP_START_TIME,
                    DRIVE_STATE.PROBE_START_TIME AS PROBE_START_TIME,
                    DRIVE_STATE.CLEANUP_START_TIME AS CLEANUP_START_TIME,
                    DRIVE_STATE.START_START_TIME AS START_START_TIME,
                    DRIVE_STATE.SHUTDOWN_TIME AS SHUTDOWN_TIME,

                    DRIVE_STATE.MOUNT_TYPE AS MOUNT_TYPE,
                    DRIVE_STATE.DRIVE_STATUS AS DRIVE_STATUS,
                    DRIVE_STATE.DESIRED_UP AS DESIRED_UP,
                    DRIVE_STATE.DESIRED_FORCE_DOWN AS DESIRED_FORCE_DOWN,
                    DRIVE_STATE.REASON_UP_DOWN AS REASON_UP_DOWN,

                    DRIVE_STATE.CURRENT_VID AS CURRENT_VID,
                    DRIVE_STATE.CTA_VERSION AS CTA_VERSION,
                    DRIVE_STATE.CURRENT_PRIORITY AS CURRENT_PRIORITY,
                    DRIVE_STATE.CURRENT_ACTIVITY AS CURRENT_ACTIVITY,
                    DRIVE_STATE.CURRENT_TAPE_POOL AS CURRENT_TAPE_POOL,
                    DRIVE_STATE.NEXT_MOUNT_TYPE AS NEXT_MOUNT_TYPE,
                    DRIVE_STATE.NEXT_VID AS NEXT_VID,
                    DRIVE_STATE.NEXT_TAPE_POOL AS NEXT_TAPE_POOL,
                    DRIVE_STATE.NEXT_PRIORITY AS NEXT_PRIORITY,
                    DRIVE_STATE.NEXT_ACTIVITY AS NEXT_ACTIVITY,

                    DRIVE_STATE.DEV_FILE_NAME AS DEV_FILE_NAME,
                    DRIVE_STATE.RAW_LIBRARY_SLOT AS RAW_LIBRARY_SLOT,

                    DRIVE_STATE.CURRENT_VO AS CURRENT_VO,
                    DRIVE_STATE.NEXT_VO AS NEXT_VO,
                    DRIVE_STATE.USER_COMMENT AS USER_COMMENT,

                    DRIVE_STATE.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,
                    DRIVE_STATE.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,
                    DRIVE_STATE.CREATION_LOG_TIME AS CREATION_LOG_TIME,
                    DRIVE_STATE.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,
                    DRIVE_STATE.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,
                    DRIVE_STATE.LAST_UPDATE_TIME AS LAST_UPDATE_TIME,

                    DRIVE_STATE.DISK_SYSTEM_NAME AS DISK_SYSTEM_NAME,
                    DRIVE_STATE.RESERVED_BYTES AS RESERVED_BYTES,
                    DRIVE_STATE.RESERVATION_SESSION_ID AS RESERVATION_SESSION_ID,
                    LOGICAL_LIBRARY.IS_DISABLED AS LOGICAL_IS_DISABLED,
                    PHYSICAL_LIBRARY.PHYSICAL_LIBRARY_NAME AS PHYSICAL_LIBRARY_NAME,
                    PHYSICAL_LIBRARY.IS_DISABLED AS PHYSICAL_IS_DISABLED
                FROM
                    DRIVE_STATE
                LEFT JOIN
                    LOGICAL_LIBRARY
                ON
                    LOGICAL_LIBRARY.LOGICAL_LIBRARY_NAME = DRIVE_STATE.LOGICAL_LIBRARY
                LEFT JOIN
                    PHYSICAL_LIBRARY
                ON
                    PHYSICAL_LIBRARY.PHYSICAL_LIBRARY_ID = LOGICAL_LIBRARY.PHYSICAL_LIBRARY_ID
                ORDER BY
                    DRIVE_NAME
                LIMIT :limit OFFSET :offset
            """
            )
            result = conn.execute(query, {"limit": limit, "offset": offset})
            rows = result.mappings().all()
            print(rows[0].keys())
            return [
                Drive(
                    bytesTransferredInSession=row["bytes_transfered_in_session"] or 0,
                    comment=row["user_comment"] or None,
                    ctaVersion=row["cta_version"] or None,
                    currentActivity=row["current_activity"] or None,
                    currentPriority=row["current_priority"] or 0,
                    desiredDriveState="UP" if row["desired_up"] else "DOWN",
                    devFileName=row["dev_file_name"] or None,
                    diskSystemName=row["disk_system_name"] or None,
                    driveName=row["drive_name"],
                    driveStatus=row["drive_status"],
                    driveStatusSince=0,  # compute later
                    filesTransferredInSession=row["files_transfered_in_session"] or 0,
                    host=row["host"],
                    logicalLibrary=row["logical_library"],
                    logicalLibraryDisabled=row["logical_is_disabled"] or False,
                    mountType=row["mount_type"],
                    physicalLibrary=row["physical_library_name"] or None,
                    physicalLibraryDisabled=row["physical_is_disabled"] or False,
                    rawLibrarySlot=row["raw_library_slot"] or None,
                    reason=row["reason_up_down"] or None,
                    reservedBytes=row["reserved_bytes"] or 0,
                    schedulerBackendName=None,  # inject separately if needed
                    sessionElapsedTime=row["session_elapsed_time"] or 0,
                    sessionId=row["session_id"] or 0,
                    tapepool=row["current_tape_pool"] or None,
                    timeSinceLastUpdate=0,  # compute later
                    vid=row["current_vid"] or None,
                    vo=row["current_vo"] or None,
                )
                for row in rows
            ]

    def get_drive(self, drive_name: str) -> Optional[Drive]:
        with self.engine.connect() as conn:
            query = text(
                """
                SELECT
                    DRIVE_STATE.DRIVE_NAME AS DRIVE_NAME,
                    DRIVE_STATE.HOST AS HOST,
                    DRIVE_STATE.LOGICAL_LIBRARY AS LOGICAL_LIBRARY,
                    DRIVE_STATE.SESSION_ID AS SESSION_ID,

                    DRIVE_STATE.BYTES_TRANSFERED_IN_SESSION AS BYTES_TRANSFERED_IN_SESSION,
                    DRIVE_STATE.FILES_TRANSFERED_IN_SESSION AS FILES_TRANSFERED_IN_SESSION,

                    DRIVE_STATE.SESSION_START_TIME AS SESSION_START_TIME,
                    DRIVE_STATE.SESSION_ELAPSED_TIME AS SESSION_ELAPSED_TIME,
                    DRIVE_STATE.MOUNT_START_TIME AS MOUNT_START_TIME,
                    DRIVE_STATE.TRANSFER_START_TIME AS TRANSFER_START_TIME,
                    DRIVE_STATE.UNLOAD_START_TIME AS UNLOAD_START_TIME,
                    DRIVE_STATE.UNMOUNT_START_TIME AS UNMOUNT_START_TIME,
                    DRIVE_STATE.DRAINING_START_TIME AS DRAINING_START_TIME,
                    DRIVE_STATE.DOWN_OR_UP_START_TIME AS DOWN_OR_UP_START_TIME,
                    DRIVE_STATE.PROBE_START_TIME AS PROBE_START_TIME,
                    DRIVE_STATE.CLEANUP_START_TIME AS CLEANUP_START_TIME,
                    DRIVE_STATE.START_START_TIME AS START_START_TIME,
                    DRIVE_STATE.SHUTDOWN_TIME AS SHUTDOWN_TIME,

                    DRIVE_STATE.MOUNT_TYPE AS MOUNT_TYPE,
                    DRIVE_STATE.DRIVE_STATUS AS DRIVE_STATUS,
                    DRIVE_STATE.DESIRED_UP AS DESIRED_UP,
                    DRIVE_STATE.DESIRED_FORCE_DOWN AS DESIRED_FORCE_DOWN,
                    DRIVE_STATE.REASON_UP_DOWN AS REASON_UP_DOWN,

                    DRIVE_STATE.CURRENT_VID AS CURRENT_VID,
                    DRIVE_STATE.CTA_VERSION AS CTA_VERSION,
                    DRIVE_STATE.CURRENT_PRIORITY AS CURRENT_PRIORITY,
                    DRIVE_STATE.CURRENT_ACTIVITY AS CURRENT_ACTIVITY,
                    DRIVE_STATE.CURRENT_TAPE_POOL AS CURRENT_TAPE_POOL,
                    DRIVE_STATE.NEXT_MOUNT_TYPE AS NEXT_MOUNT_TYPE,
                    DRIVE_STATE.NEXT_VID AS NEXT_VID,
                    DRIVE_STATE.NEXT_TAPE_POOL AS NEXT_TAPE_POOL,
                    DRIVE_STATE.NEXT_PRIORITY AS NEXT_PRIORITY,
                    DRIVE_STATE.NEXT_ACTIVITY AS NEXT_ACTIVITY,

                    DRIVE_STATE.DEV_FILE_NAME AS DEV_FILE_NAME,
                    DRIVE_STATE.RAW_LIBRARY_SLOT AS RAW_LIBRARY_SLOT,

                    DRIVE_STATE.CURRENT_VO AS CURRENT_VO,
                    DRIVE_STATE.NEXT_VO AS NEXT_VO,
                    DRIVE_STATE.USER_COMMENT AS USER_COMMENT,

                    DRIVE_STATE.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,
                    DRIVE_STATE.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,
                    DRIVE_STATE.CREATION_LOG_TIME AS CREATION_LOG_TIME,
                    DRIVE_STATE.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,
                    DRIVE_STATE.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,
                    DRIVE_STATE.LAST_UPDATE_TIME AS LAST_UPDATE_TIME,

                    DRIVE_STATE.DISK_SYSTEM_NAME AS DISK_SYSTEM_NAME,
                    DRIVE_STATE.RESERVED_BYTES AS RESERVED_BYTES,
                    DRIVE_STATE.RESERVATION_SESSION_ID AS RESERVATION_SESSION_ID,
                    LOGICAL_LIBRARY.IS_DISABLED AS LOGICAL_IS_DISABLED,
                    PHYSICAL_LIBRARY.PHYSICAL_LIBRARY_NAME AS PHYSICAL_LIBRARY_NAME,
                    PHYSICAL_LIBRARY.IS_DISABLED AS PHYSICAL_IS_DISABLED
                FROM
                    DRIVE_STATE
                LEFT JOIN
                    LOGICAL_LIBRARY
                ON
                    LOGICAL_LIBRARY.LOGICAL_LIBRARY_NAME = DRIVE_STATE.LOGICAL_LIBRARY
                LEFT JOIN
                    PHYSICAL_LIBRARY
                ON
                    PHYSICAL_LIBRARY.PHYSICAL_LIBRARY_ID = LOGICAL_LIBRARY.PHYSICAL_LIBRARY_ID
                WHERE
                    DRIVE_NAME = :drive_name
                ORDER BY
                    DRIVE_NAME
            """
            )
            result = conn.execute(query, {"drive_name": drive_name.strip()})
            row = result.mappings().first()
            if row is None:
                return None
            return Drive(
                bytesTransferredInSession=row["bytes_transfered_in_session"] or 0,
                comment=row["user_comment"] or None,
                ctaVersion=row["cta_version"] or None,
                currentActivity=row["current_activity"] or None,
                currentPriority=row["current_priority"] or 0,
                desiredDriveState="UP" if row["desired_up"] else "DOWN",
                devFileName=row["dev_file_name"] or None,
                diskSystemName=row["disk_system_name"] or None,
                driveName=row["drive_name"],
                driveStatus=row["drive_status"],
                driveStatusSince=0,  # compute later
                filesTransferredInSession=row["files_transfered_in_session"] or 0,
                host=row["host"],
                logicalLibrary=row["logical_library"],
                logicalLibraryDisabled=row["logical_is_disabled"] or False,
                mountType=row["mount_type"],
                physicalLibrary=row["physical_library_name"] or None,
                physicalLibraryDisabled=row["physical_is_disabled"] or False,
                rawLibrarySlot=row["raw_library_slot"] or None,
                reason=row["reason_up_down"] or None,
                reservedBytes=row["reserved_bytes"] or 0,
                schedulerBackendName=None,  # inject separately if needed
                sessionElapsedTime=row["session_elapsed_time"] or 0,
                sessionId=row["session_id"] or 0,
                tapepool=row["current_tape_pool"] or None,
                timeSinceLastUpdate=0,  # compute later
                vid=row["current_vid"] or None,
                vo=row["current_vo"] or None,
            )

    def set_drive_up(self, drive_name: str, reason: str) -> bool:
        with self.engine.begin() as conn:
            query = text(
                """"
                UPDATE DRIVE_STATE SET
                    DESIRED_UP = true,
                    DESIRED_FORCE_DOWN = false,
                    REASON_UP_DOWN = :reason_up_down
                WHERE DRIVE_NAME = :drive_name
            """
            )
            result = conn.execute(
                query,
                {
                    "drive_name": drive_name.strip(),
                    "reason_up_down": reason.strip(),
                },
            )
            if result.rowcount == 0:
                return False
            return True

    def set_drive_down(self, drive_name: str, reason: str, force: bool = False) -> bool:
        with self.engine.begin() as conn:
            query = text(
                """"
                UPDATE DRIVE_STATE SET
                    DESIRED_UP = false,
                    DESIRED_FORCE_DOWN = :desired_force_down,
                    REASON_UP_DOWN = :reason_up_down
                WHERE DRIVE_NAME = :drive_name
            """
            )
            result = conn.execute(
                query, {"drive_name": drive_name.strip(), "reason_up_down": reason.strip(), "desired_force_down": force}
            )
            if result.rowcount == 0:
                return False
            return True

    def update_drive_comment(self, drive_name: str, comment: str) -> bool:
        with self.engine.begin() as conn:
            query = text(
                """"
                UPDATE DRIVE_STATE SET
                    USER_COMMENT = user_comment,
                WHERE DRIVE_NAME = :drive_name
            """
            )
            result = conn.execute(query, {"drive_name": drive_name.strip(), "user_comment": comment.strip()})
            if result.rowcount == 0:
                return False
            return True
