# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Shared fixtures and model factories for the Cli unit tests.

The Cli layer is tested against a mocked [CtaClient][cta.sdk.client.protocol.CtaClient]:
commands are exercised through Click's ``CliRunner`` and the assertions target the Cli
behaviour (argument parsing, guard clauses, the exact Dao call, printed feedback) rather
than the backend, which owns its own test-suite.
"""

from datetime import datetime, timedelta
from typing import Any, Optional
from unittest.mock import MagicMock

import click
import pytest
from click.testing import CliRunner, Result
from cta.sdk.client.protocol import CtaClient
from cta.sdk.models import (
    Drive,
    DriveConfig,
    DriveMetadata,
    DriveOperatingState,
    Metadata,
    MountType,
    Tape,
    TapeMetadata,
    TapePool,
    TapePoolStatistics,
    TapeSession,
    TapeSessionState,
    TapeState,
    TapeStatistics,
    UserRequest,
)
from ctaops.cli.config import AppConfig, DisplayConfig
from ctaops.cli.context import AppContext

CREATION_TIME = datetime(2024, 1, 1, 9, 0, 0)
UPDATE_TIME = datetime(2024, 6, 1, 12, 0, 0)


def make_metadata(comment: str = "a comment") -> Metadata:
    """Build a [Metadata][cta.sdk.models.metadata.Metadata] with fixed timestamps."""
    return Metadata(
        creation_user_name="ctaops",
        creation_host_name="tpsrv001",
        creation_time=CREATION_TIME,
        update_user_name="ctaops",
        update_host_name="tpsrv001",
        update_time=UPDATE_TIME,
        comment=comment,
    )


def make_tape(vid: str = "V00001", **overrides: Any) -> Tape:
    """Build a [Tape][cta.sdk.models.tape.Tape] with sensible defaults."""
    statistics = overrides.pop(
        "statistics",
        TapeStatistics(
            physical_usage=0,
            logical_usage=0,
            is_dirty=False,
            last_fseq=0,
            is_full=False,
            file_count=0,
        ),
    )
    metadata = overrides.pop(
        "metadata",
        TapeMetadata(
            **vars(make_metadata()),
            label_time=None,
            label_drive_name=None,
            last_write_time=None,
            last_write_drive_name=None,
            last_read_time=None,
            last_read_drive_name=None,
            update_state_time=UPDATE_TIME,
            update_state_info="",
            write_mount_count=0,
            read_mount_count=0,
        ),
    )
    defaults: dict[str, Any] = {
        "media_type_name": "LTO9",
        "vendor": "IBM",
        "purchase_order": "PO-1",
        "logical_library_name": "LL1",
        "physical_library_name": "PL1",
        "tape_pool_name": "test_pool",
        "virtual_organisation_name": "vo1",
        "encryption_key_name": None,
        "capacity": 18_000_000_000_000,
        "format": "CTA",
        "state": TapeState.ACTIVE,
        "state_reason": "",
        "verification_status": None,
    }
    defaults.update(overrides)
    return Tape(vid=vid, statistics=statistics, metadata=metadata, **defaults)


def make_tape_session(session_id: int = 1, **overrides: Any) -> TapeSession:
    """Build a [TapeSession][cta.sdk.models.tape_session.TapeSession]."""
    defaults: dict[str, Any] = {
        "vid": "V00001",
        "tape_pool_name": "test_pool",
        "mount_type": MountType.ARCHIVE,
        "file_transferred_count": 10,
        "byte_transferred_count": 1_000_000,
        "virtual_organisation_name": "vo1",
        "state": TapeSessionState.TRANSFERRING,
        "duration": timedelta(minutes=5),
    }
    defaults.update(overrides)
    return TapeSession(id=session_id, **defaults)


def make_drive(name: str = "DRIVE01", **overrides: Any) -> Drive:
    """Build a [Drive][cta.sdk.models.drive.Drive] with sensible defaults."""
    metadata = overrides.pop(
        "metadata",
        DriveMetadata(
            last_refresh_age=timedelta(seconds=30),
            cta_version="5.0",
            scheduler_namespace="default",
            comment="a drive",
        ),
    )
    defaults: dict[str, Any] = {
        "logical_library_name": "LL1",
        "host_name": "tpsrv001.cern.ch",
        "desired_state": DriveOperatingState.UP,
        "state": DriveOperatingState.UP,
        "state_reason": "",
        "device_path": "/dev/nst0",
        "physical_library_name": "PL1",
        "physical_library_slot": "1",
        "tape_session": None,
        "config": DriveConfig({}),
    }
    defaults.update(overrides)
    return Drive(name=name, metadata=metadata, **defaults)


def make_tape_pool(name: str = "test_pool", **overrides: Any) -> TapePool:
    """Build a [TapePool][cta.sdk.models.tape_pool.TapePool]."""
    statistics = overrides.pop("statistics", TapePoolStatistics(tape_count=2, file_count=10, capacity=2000, usage=500))
    metadata = overrides.pop("metadata", make_metadata())
    defaults: dict[str, Any] = {
        "virtual_organisation_name": "vo1",
        "min_writable_tape_count": 1,
        "encryption_key_name": None,
        "supply_tape_pool_names": [],
    }
    defaults.update(overrides)
    return TapePool(name=name, statistics=statistics, metadata=metadata, **defaults)


def make_user_request(**overrides: Any) -> UserRequest:
    """Build a [UserRequest][cta.sdk.models.user_request.UserRequest]."""
    defaults: dict[str, Any] = {
        "mount_type": MountType.ARCHIVE,
        "tape_pool_name": "test_pool",
        "vid": "V00001",
        "virtual_organisation_name": "vo1",
        "logical_library_name": "LL1",
        "queued_file_count": 5,
        "queued_byte_count": 1_000,
        "oldest_age": timedelta(minutes=10),
        "priority": 1,
        "min_age": timedelta(minutes=1),
        "mount_count": 1,
        "transferred_file_count": 1,
        "transferred_byte_count": 100,
        "is_sleeping_for_space": False,
        "sleep_start_time": None,
        "scheduler_backend_name": "default",
        "tapes_file_count": 10,
        "tapes_byte_count": 2_000,
        "tapes_capacity": 10_000,
    }
    defaults.update(overrides)
    return UserRequest(**defaults)


@pytest.fixture
def cta_client() -> MagicMock:
    """Return a mocked CtaClient whose DAOs reject unknown attributes."""
    return MagicMock(spec=CtaClient)


@pytest.fixture
def app_config() -> AppConfig:
    """Return an AppConfig with no log file or backend."""
    return AppConfig(display=DisplayConfig(format="table"))


@pytest.fixture
def app_context(app_config: AppConfig, cta_client: MagicMock) -> AppContext:
    """Build the context passed to commands through ``@click.pass_obj``."""
    return AppContext(config=app_config, cta_client=cta_client)


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def invoke(runner: CliRunner, app_context: AppContext):
    """Invoke a command with the ``app_context`` fixture already wired in.

    Args:
        command: The Click command or group to run.
        args: Command line arguments.
        obj: Optional AppContext override.
    """

    def _invoke(
        command: click.Command, args: Optional[list[str]] = None, obj: Optional[AppContext] = None, **kwargs: Any
    ) -> Result:
        return runner.invoke(command, args or [], obj=obj if obj is not None else app_context, **kwargs)

    return _invoke
