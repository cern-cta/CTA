# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Configuration dataclasses for CTA Operations Cli.

This module defines the application configuration structure, including
display settings, backend selection, and logging configuration.
"""

from dataclasses import dataclass, field
from typing import Literal, Optional

from cta.sdk.client.grpc.models import WorkflowConfig
from ctaops.utils.config import NOT_SET
from ctaops.utils.logging import LoggingConfiguration


@dataclass
class UdevPropertiesMap:
    library_name_key: str = "CTAOPS_NAME"
    drive_name_key: str = "CTAOPS_NAME"
    drive_library_address_key: str = "CTAOPS_LIBRARY_ADDRESS"
    drive_library_ordinal_key: str = "CTAOPS_LIBRARY_ORDINAL"


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
class FileBackendConfig:
    """Configuration for File backend execution.

    Attributes:
        config_file: Path to the configuration file for file-based operations.
    """

    root_dir: str = NOT_SET


@dataclass
class BackendsConfig:
    """Configuration container for supported backends.

    Attributes:
        CLI: Configuration for CLI backend operations.
    """

    CLI: AdminBackendConfig = field(default_factory=AdminBackendConfig)
    FILE: FileBackendConfig = field(default_factory=FileBackendConfig)


@dataclass
class TableColumnsConfig:
    """Table column overrides.

    You can provide a list of fields to force-include and/or
    a list fields to force-exclude in the table output.

    Attributes:
        table_name: The name of the dataclass type in PascalCase
        exclude_fields: A list of attributes to exclude from the table output (CTA sdk naming convention with . separation)
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
    pretty_get: bool = False
    columns: list[TableColumnsConfig] = field(default_factory=list)


@dataclass
class AppConfig:
    """Main application configuration.

    This dataclass aggregates all configuration sections for the Cli application,
    including display settings, logging, backend selection, and backend-specific configs.

    Attributes:
        display: Display configuration for output formatting.
        logging: Logging configuration for console and file logging.
        backend: Selected backend type (currently only "CLI" is supported).
        backends: Container for all backend configurations.
        cta_catalogue_conn_file: Path to CTA catalogue connection configuration file.
        tape_server: Configuration for tape server interactions.
        workflow: Optional configuration for the workflow frontend.
    """

    display: DisplayConfig = field(default_factory=DisplayConfig)
    logging: LoggingConfiguration = field(default_factory=lambda: LoggingConfiguration(level="INFO"))
    backend: Literal["CLI", "FILE"] = "CLI"
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
        if self.backend not in ["FILE", "CLI"]:
            raise ValueError(f"Unknown backend {self.backend!r} selected, possible options are: FILE, CLI ")
        if self.backend == "FILE" and self.backends.FILE.root_dir is NOT_SET:
            raise ValueError("FILE backend selected but not root_dir config provided")
        return self
