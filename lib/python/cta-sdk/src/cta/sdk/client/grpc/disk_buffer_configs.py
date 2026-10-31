# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Predefined disk buffer URLs for testing/troubleshooting."""

import typing as t
from functools import lru_cache

from cta.sdk.client.grpc.exceptions import WorkflowConfigError

if t.TYPE_CHECKING:
    from cta.sdk.client.grpc.models import WorkflowConfig, WorkflowFileInfo


def get_disk_buffer_mode_url(
    key: str, workflow_cfg: "WorkflowConfig", file_info: "WorkflowFileInfo"
) -> t.Optional[str]:
    """Resolve a named disk-buffer URL from the workflow configuration."""
    if workflow_cfg.disk_buffer_mode is None:
        return None

    if workflow_cfg.disk_buffer_mode == "eos-test":
        return eos_test(
            workflow_cfg.disk_endpoint,
            file_info.archive_file_id,
            file_info.path,
            file_info.fxid,
        )[key]

    raise WorkflowConfigError(f"Unknown disk buffer mode '{workflow_cfg.disk_buffer_mode}'")


def _build_eos_query(disk_endpoint: str, query: dict[str, object], schema: str = "eosQuery"):
    """Build an EOS query URL for the given file info."""
    query_str = "&".join(f"{k}={v}" for k, v in query.items())
    return f"{schema}://{disk_endpoint}?{query_str}"


@lru_cache
def eos_test(disk_endpoint: str, archive_file_id: int, path: str, fxid: str) -> dict[str, str]:
    return {
        "service_url": _build_eos_query(
            f"{disk_endpoint}/{path}",
            {
                "eos.lfn": f"fxid:{fxid}",
            },
            schema="root",
        ),
        "archive_report_url": _build_eos_query(
            disk_endpoint,
            {
                "mgm.pcmd": "event",
                "mgm.fid": fxid,
                "mgm.logid": "cta",
                "mgm.event": "sync::archived",
                "mgm.workflow": "default",
                "mgm.path": "/dummy_path",
                "mgm.ruid": "0",
                "mgm.rgid": "0",
                "cta_archive_file_id": archive_file_id,
            },
        ),
        "archive_error_report_url": _build_eos_query(
            f"{disk_endpoint}//eos/wfe/passwd",
            {
                "mgm.pcmd": "event",
                "mgm.fid": fxid,
                "mgm.logid": "cta",
                "mgm.event": "sync::archive_failed",
                "mgm.workflow": "default",
                "mgm.path": "/dummy_path",
                "mgm.ruid": "0",
                "mgm.rgid": "0",
                "cta_archive_file_id": archive_file_id,
                "mgm.errmsg": "",
            },
        ),
        "prepare_dest_url": _build_eos_query(
            f"{disk_endpoint}/{path}",
            {
                "eos.lfn": f"fxid:{fxid}",
                "eos.ruid": "0",
                "eos.rgid": "0",
                "eos.injection": "1",
                "eos.workflow": "retrieve_written",
                "eos.space": "default",
            },
            schema="root",
        ),
        "prepare_error_report_url": _build_eos_query(
            f"{disk_endpoint}//eos/wfe/passwd",
            {
                "mgm.pcmd": "event",
                "mgm.fid": fxid,
                "mgm.logid": "cta",
                "mgm.event": "sync::retrieve_failed",
                "mgm.workflow": "default",
                "mgm.path": "/dummy_path",
                "mgm.ruid": "0",
                "mgm.rgid": "0",
                "mgm.errmsg": "",
            },
        ),
    }
