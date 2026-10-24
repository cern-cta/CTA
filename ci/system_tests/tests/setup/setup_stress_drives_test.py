# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Setup test for stress-drive mode.

Registers the virtual tape libraries in the CTA catalogue.  Must be run after
setup_cta_test.py (catalogue and admin users must already exist).

Applies only to deployments that include at least one taped pod configured
with DriveDevice = "virtual://<base_dir>".  The entire module is skipped
automatically when no virtual drives are present.

Library configuration (media types, tape counts, VID prefixes, capacities) is
read directly from each library.toml on the mounted shared volumes, so no
manual duplication into test_params.toml is required.
"""

import json
import time

import pytest

from system_tests.helpers.hosts import CtaCliHost, CtaTapedHost
from system_tests.helpers.hosts.cta_taped_host import StressLibraryConf
from system_tests.helpers.test_env import TestEnv


# =========================================================================
#  Helpers
# =========================================================================


def _stress_tapeds(env: TestEnv) -> list[CtaTapedHost]:
    """Return only the taped hosts running in stress mode."""
    return [t for t in env.cta_taped if t.is_stress_mode]


def _vid(prefix: str, index: int) -> str:
    """Format a VID from a prefix and a 4-digit zero-padded index."""
    return f"{prefix}{index:04d}"


# =========================================================================
#  Module-level skip guard
# =========================================================================


@pytest.fixture(autouse=True)
def skip_if_no_stress_drives(env: TestEnv) -> None:
    """Skip every test in this module when no stress-mode taped pods are present."""
    if not _stress_tapeds(env):
        pytest.skip("No stress-mode taped pods found; skipping stress-drive setup")


# =========================================================================
#  Library config fixture
# =========================================================================


@pytest.fixture(scope="session")
def stress_library_confs(env: TestEnv) -> list[StressLibraryConf]:
    """Read the library config from one taped pod per unique logical library.

    Drives are split across N logical libraries (stress-lib-0 .. stress-lib-{N-1}).
    Each taped pod's DRIVE_DEVICE URI encodes its library root, and its
    LOGICAL_LIBRARY_NAME identifies which library it belongs to.  We read
    library.toml from one representative pod per library and return the configs
    sorted by library name for deterministic ordering.
    """
    tapeds = _stress_tapeds(env)
    if not tapeds:
        pytest.skip("No stress-mode taped pods found")
    # One representative pod per unique logical library name.
    seen: dict[str, CtaTapedHost] = {}
    for t in tapeds:
        lib = t.logical_library_name
        if lib not in seen:
            seen[lib] = t
    return [seen[lib].read_stress_library_conf() for lib in sorted(seen, key=lambda n: int(n.rsplit("-", 1)[1]))]


# =========================================================================
#  Setup tests
# =========================================================================


def test_register_stress_media_types(
    cta_cli: CtaCliHost,
    stress_library_confs: list[StressLibraryConf],
) -> None:
    """Register media types declared in library.toml (shared across all libraries)."""
    for mt in stress_library_confs[0].media_types:
        cta_cli.exec(
            f"cta-admin mediatype add"
            f" --name {mt.name}"
            f" --capacity {mt.capacity_bytes}"
            f" --cartridge STRESS"
            f" --comment 'Virtual stress-test tape'"
        )


def test_register_stress_logical_libraries(
    cta_cli: CtaCliHost,
    stress_library_confs: list[StressLibraryConf],
) -> None:
    """Register all logical libraries that stress drives and tapes belong to."""
    for conf in stress_library_confs:
        cta_cli.exec(
            f"cta-admin logicallibrary add"
            f" --name {conf.name}"
            f" --comment 'Virtual logical library for stress drives'"
        )


def test_add_stress_tapes(
    cta_cli: CtaCliHost,
    stress_library_confs: list[StressLibraryConf],
) -> None:
    """Register all virtual tapes across every library in the CTA catalogue."""
    for conf in stress_library_confs:
        for mt in conf.media_types:
            for i in range(1, mt.count + 1):
                vid = _vid(mt.vid_prefix, i)
                cta_cli.exec(
                    f"cta-admin tape add"
                    f" --mediatype {mt.name}"
                    f" --purchaseorder stress-order"
                    f" --vendor stress-vendor"
                    f" --logicallibrary {conf.name}"
                    f" --tapepool ctasystest"
                    f" --vid {vid}"
                    f" --full false"
                    f" --comment 'Virtual stress tape'"
                )


def test_set_stress_drives_up(cta_cli: CtaCliHost, env: TestEnv) -> None:
    """Bring all stress drives up so the scheduler can dispatch work to them."""
    stress_pods = _stress_tapeds(env)
    expected_drives = {pod.drive_name for pod in stress_pods}

    timeout_secs = 120
    deadline = time.monotonic() + timeout_secs
    while True:
        registered = {d["driveName"] for d in json.loads(cta_cli.exec_with_output("cta-admin --json drive ls"))}
        missing = expected_drives - registered
        if not missing:
            break
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"Stress drives not registered in CTA after {timeout_secs} s. Missing: {sorted(missing)}"
            )
        time.sleep(2)

    for pod in stress_pods:
        drive_name = pod.drive_name
        print(f"Bringing stress drive {drive_name} up")
        cta_cli.exec(f"cta-admin drive up {drive_name} --reason 'stress-drive setup'")
