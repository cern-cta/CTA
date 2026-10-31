# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class CtaCliVersion:
    """CTA CLI version (cta-admin).

    Attributes:
        version: The version of cli client used
        xrootd_ssi_protobuf_version: xrootD interface version.
    """

    version: str
    xrootd_ssi_protobuf_version: Optional[str]


@dataclass(frozen=True)
class CtaServerVersion:
    """CTA server version (frontend).

    Attributes:
        version: The version of cta frontend used
        xrootd_ssi_protobuf_version: xrootD interface version
    """

    version: str
    xrootd_ssi_protobuf_version: Optional[str]


@dataclass(frozen=True)
class CtaVersion:
    """CTA client and server configuration information.

    Attributes:
        cli_version: The version of cli client used
        server_version: The version of frontend used
        catalogue_connection_string: Connection string of catalogue used by the frontend
        catalogue_version: Version of CTA's catalogue
        is_upgrading: Boolean
        scheduler_backend_name: Scheduler's name used by the frontend
        instance_name: Namespace used by current deployment
    """

    cli_version: CtaCliVersion
    server_version: CtaServerVersion
    catalogue_connection_string: str
    catalogue_version: str
    is_upgrading: bool
    scheduler_backend_name: str
    instance_name: str
