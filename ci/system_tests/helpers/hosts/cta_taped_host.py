# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import sys
from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

from system_tests.helpers.connections.remote_connection import RemoteConnection

from .remote_host import RemoteHost


@dataclass
class MediaTypeConf:
    """Properties of one media type as declared in library.toml."""

    name: str
    capacity_bytes: int
    vid_prefix: str
    mount_delay_ms: int
    count: int


@dataclass
class StressLibraryConf:
    """Parsed contents of a stress-drive library.toml file."""

    name: str
    changer_timeout_ms: int
    media_types: list[MediaTypeConf] = field(default_factory=list)


class CtaTapedHost(RemoteHost):
    def __init__(self, conn: RemoteConnection) -> None:
        super().__init__(conn)

    @cached_property
    def log_file_path(self) -> Path:
        return Path("/var") / "log" / "cta" / "cta-taped.log"

    @cached_property
    def drive_name(self) -> str:
        return self.exec_with_output("printenv DRIVE_NAME")

    @cached_property
    def process_name(self) -> str:
        return "taped"

    @cached_property
    def drive_device(self) -> str:
        device: str = self.exec_with_output("printenv DRIVE_DEVICE")
        if not device.startswith("/dev/") and "://" not in device:
            device = "/dev/" + device
        return device

    @cached_property
    def drive_control_path(self) -> str:
        return self.exec_with_output("printenv DRIVE_CONTROL_PATH")

    @cached_property
    def drive_index(self) -> int:
        control_path = self.drive_control_path
        index = control_path.removeprefix("smc")
        if control_path.startswith("smc") and index.isdigit():
            return int(index)
        raise RuntimeError(f"Could not determine drive index from DRIVE_CONTROL_PATH={control_path}")

    @cached_property
    def library_device(self) -> str:
        device: str = self.exec_with_output("printenv LIBRARY_DEVICE")
        if not device.startswith("/dev/"):
            device = "/dev/" + device
        return device

    @cached_property
    def logical_library_name(self) -> str:
        return self.exec_with_output("printenv LOGICAL_LIBRARY_NAME")

    def label_tapes(self, tapes: list[str]) -> None:
        for tape in tapes:
            self.label_tape(tape)

    def label_tape(self, tape: str) -> None:
        self.exec(f"cta-tape-label --vid {tape} --force")

    @cached_property
    def is_stress_mode(self) -> bool:
        """True when this taped pod is configured with StressMode = yes (no real SCSI hardware)."""
        device = self.exec_with_output("printenv DRIVE_DEVICE")
        return device == "stress://"

    @cached_property
    def stress_base_dir(self) -> Path:
        """Root of the shared fake-tape-library volume (value of StressBaseDir in cta-taped.conf)."""
        raw = self.exec_with_output('awk \'$1=="taped" && $2=="StressBaseDir" {print $3}\' /etc/cta/cta-taped.conf')
        return Path(raw)

    def read_stress_library_conf(self) -> StressLibraryConf:
        """Read and parse the library.toml from the shared volume mounted in this pod."""
        conf_path = self.stress_base_dir / "config" / self.logical_library_name / "library.toml"
        raw = self.exec_with_output(f"cat '{conf_path}'")
        data = tomllib.loads(raw)
        lib = data["library"]
        media_types = [
            MediaTypeConf(
                name=mt["name"],
                capacity_bytes=mt["capacityBytes"],
                vid_prefix=mt["vidPrefix"],
                mount_delay_ms=mt.get("mountDelayMs", 0),
                count=mt["count"],
            )
            for mt in lib.get("mediaTypes", [])
        ]
        return StressLibraryConf(
            name=lib["name"],
            changer_timeout_ms=lib.get("changerTimeoutMs", 30_000),
            media_types=media_types,
        )
