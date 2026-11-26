# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Models used by the CTA workflow gRPC client."""

import typing as t
from dataclasses import dataclass
from pathlib import Path


@dataclass
class WorkflowFileInfo:
    """File metadata used to build workflow requests."""

    archive_file_id: str | None = None
    id: int | None = None
    path: str | None = None
    uid: str | None = None
    gid: str | None = None
    size: int | None = None
    checksum: str | None = None
    xattr: dict[str, str] | None = None

    @property
    def fxid(self) -> str | None:
        if self.id is None:
            return None

        return f"{self.id:>08x}"


@dataclass
class WorkflowConfig:
    """Configuration for the workflow gRPC endpoint.

    Attributes:
        disk_buffer_mode: predefined disk buffer URLs, for testing/troubleshooting.
        frontend_endpoint: gRPC server endpoint for CTA WFE services.
        disk_endpoint: disk buffer endpoint (e.g. EOS instance)
        disk_instance: name of the disk instance sending the events.
        jwt_token_file: Optional path to a file containing the JWT used to authenticate.
        timeout: timeout in seconds.
        verify_cert: Whether to verify the server's TLS certificate against system roots.
        ca_cert_file: Optional path to a custom CA/server cert PEM file for TLS trust.
        server_name_override: Optional TLS server name override for hostname mismatch testing.
    """

    disk_buffer_mode: t.Literal["eos-test"] | None = None
    frontend_endpoint: str | None = "localhost:10956"
    disk_endpoint: str = "localhost:10955"
    disk_instance: str = "ctaeos"
    jwt_token_file: Path | None = None
    timeout: int = 30
    verify_cert: bool = True
    ca_cert_file: Path | None = None
    server_name_override: str | None = None
