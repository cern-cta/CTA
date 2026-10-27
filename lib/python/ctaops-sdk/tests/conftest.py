"""Shared pytest fixtures for ctaops-sdk unit tests.

The SDK interacts with hardware through pyudev and external SCSI/tape
binaries via subprocess. Fixtures here provide builder helpers to fabricate
fake udev Devices and subprocess.CompletedProcess results so tests can
exercise the SDK without root privileges or real tape hardware.
"""

import subprocess
from typing import Any, Iterable, Optional
from unittest.mock import MagicMock

import pytest


def make_device(
    *,
    sys_name: str = "nst0",
    subsystem: str = "scsi_tape",
    device_node: Optional[str] = "/dev/nst0",
    properties: Optional[dict[str, Any]] = None,
    parent: Optional[Any] = None,
) -> MagicMock:
    """Build a MagicMock that mimics the parts of pyudev.Device the SDK uses."""
    device = MagicMock(name=f"Device({sys_name})")
    device.sys_name = sys_name
    device.subsystem = subsystem
    device.device_node = device_node
    device.parent = parent if parent is not None else MagicMock(name=f"Parent({sys_name})")
    device.find_parent.return_value = device.parent

    properties = dict(properties or {})
    properties_mock = MagicMock(name=f"Properties({sys_name})")
    properties_mock.get.side_effect = lambda key, default=None: properties.get(key, default)
    properties_mock.__contains__.side_effect = lambda key: key in properties
    properties_mock.__getitem__.side_effect = lambda key: properties[key]
    device.properties = properties_mock
    device._properties_dict = properties
    return device


def make_completed_process(
    args: Iterable[str] = (),
    returncode: int = 0,
    stdout: str = "",
    stderr: str = "",
) -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=list(args), returncode=returncode, stdout=stdout, stderr=stderr)


@pytest.fixture
def device_factory():
    return make_device


@pytest.fixture
def completed_process_factory():
    return make_completed_process


@pytest.fixture
def make_context():
    """Return a builder for a fake pyudev.Context backed by a list of devices."""

    def _build(devices: list[MagicMock]) -> MagicMock:
        ctx = MagicMock(name="Context")

        def list_devices(**filters: Any) -> list[MagicMock]:
            results = []
            for device in devices:
                if "subsystem" in filters and device.subsystem != filters["subsystem"]:
                    continue
                if "parent" in filters and device.parent is not filters["parent"]:
                    continue
                ok = True
                for key, value in filters.items():
                    if key in {"subsystem", "parent"}:
                        continue
                    if device._properties_dict.get(key) != value:
                        ok = False
                        break
                if ok:
                    results.append(device)
            return results

        ctx.list_devices.side_effect = list_devices
        return ctx

    return _build
