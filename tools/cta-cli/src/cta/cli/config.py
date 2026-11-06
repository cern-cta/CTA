# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Configuration dataclasses for CTA Operations Cli.

This module defines the application configuration structure, including
display settings, backend selection, and logging configuration.
"""

from dataclasses import dataclass, field
from typing import Literal, Optional

from cta.common.config import NOT_SET
from cta.common.logging import LoggingConfiguration
from cta.sdk.client.grpc.models import WorkflowConfig


@dataclass
class UdevPropertiesMap:
    library_name_key: str = "CTA_NAME"
    drive_name_key: str = "CTA_NAME"
    drive_library_address_key: str = "CTA_LIBRARY_ADDRESS"
    drive_library_ordinal_key: str = "CTA_LIBRARY_ORDINAL"


@dataclass
class TapeServerConfig:
    udev_properties_map: UdevPropertiesMap = field(default_factory=UdevPropertiesMap)


@dataclass
class AdminBackendConfig:
    """Configuration for Cli backend execution.

    Attributes:
        exec_path: Path to the cta-admin executable for backend operations.
        timeout: Command execution timeout in seconds.
        retry: Number of retry attempts for failed operations.
        sss_keytab_file: Path to Kerberos keytab file for authentication.
        cta_conf_file: Path to CTA Cli configuration file.
    """

    exec_path: str = "/usr/bin/cta-admin"
    timeout: int = 60
    sss_keytab_file: Optional[str] = None
    cta_conf_file: Optional[str] = None


@dataclass
class MockBackendConfig:
    """Configuration for MOCK backend execution.

    Attributes:
        root_dir: Directory where the MOCK backend stores its JSON files.
    """

    root_dir: str = NOT_SET


@dataclass
class BackendsConfig:
    """Configuration container for supported backends.

    Attributes:
        CTA_ADMIN: Configuration for the CTA_ADMIN backend.
        MOCK: Configuration for the MOCK backend.
    """

    CTA_ADMIN: AdminBackendConfig = field(default_factory=AdminBackendConfig)
    MOCK: MockBackendConfig = field(default_factory=MockBackendConfig)


@dataclass
class TableColumnsConfig:
    """
    Table column overrides.

    You can provide a list of fields to force-include and/or
    a list fields to force-exclude in the table output.

    Attributes:
        table_name: The name of the dataclass type in PascalCase
        exclude_fields: A list of attributes to exclude from the table output (CTA sdk naming convention with .
            separation)
    """

    table_name: str
    exclude_fields: list[str] = field(default_factory=list)


@dataclass
class DisplayConfig:
    """Display output configuration.

    Attributes:
        format: Output format - "table" for tabular display, "json" for JSON,
            or "raw" for raw object representation.
    """

    format: Literal["table", "json", "raw"] = "table"
    chunk_size: int = 1000  # Number of objects to include in each chunk for display
    pretty_table: bool = True
    pretty_json: bool = True
    columns: list[TableColumnsConfig] = field(default_factory=list)


@dataclass
class AppConfig:
    """Main application configuration.

    This dataclass aggregates all configuration sections for the Cli application,
    including display settings, logging, backend selection, and backend-specific configs.

    Attributes:
        display: Display configuration for output formatting.
        logging: Logging configuration for console and file logging.
        backend: Selected backend type ("CTA_ADMIN" or "MOCK").
        backends: Container for all backend configurations.
        cta_catalogue_conn_file: Path to CTA catalogue connection configuration file.
        tape_server: Configuration for tape server interactions.
        workflow: Optional configuration for the workflow frontend.
    """

    display: DisplayConfig = field(default_factory=DisplayConfig)
    logging: LoggingConfiguration = field(default_factory=lambda: LoggingConfiguration(level="INFO"))
    backend: Literal["CTA_ADMIN", "MOCK"] = "CTA_ADMIN"
    backends: BackendsConfig = field(default_factory=BackendsConfig)
    cta_catalogue_conn_file: str = "/etc/cta/cta-catalogue.conf"
    tape_server: TapeServerConfig = field(default_factory=TapeServerConfig)
    workflow: WorkflowConfig = field(default_factory=WorkflowConfig)

    def validate(self) -> "AppConfig":
        """Validate the configuration.

        Performs any necessary validation of configuration values.

        Returns:
            Self for method chaining.
        """
        if self.backend not in ["MOCK", "CTA_ADMIN"]:
            raise ValueError(f"Unknown backend {self.backend!r} selected, possible options are: MOCK, CTA_ADMIN")
        if self.backend == "MOCK" and self.backends.MOCK.root_dir is NOT_SET:
            raise ValueError("MOCK backend selected but not root_dir config provided")
        return self
