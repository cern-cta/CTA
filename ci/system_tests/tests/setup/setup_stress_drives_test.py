# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Setup test for stress-drive mode.

Registers the virtual tape library in the CTA catalogue.  Must be run after
setup_cta_test.py (catalogue and admin users must already exist).

Applies only to deployments that include at least one taped pod configured
with DriveDevice = "virtual://<base_dir>".  The entire module is skipped
automatically when no virtual drives are present.

Library configuration (media types, tape counts, VID prefixes, capacities) is
read directly from library.toml on the mounted shared volume, so no manual
duplication into test_params.toml is required.
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
    """Skip every test in this module when no stress-mode taped pods are present.

    Using an autouse fixture is the correct pytest pattern for a module-wide
    runtime skip: a plain pytest.skip() call inside a test function only skips
    that one test, allowing later tests to run and register catalogue entries
    (tapes, media types) that pollute the catalogue for non-stress deployments.
    """
    if not _stress_tapeds(env):
        pytest.skip("No stress-mode taped pods found; skipping stress-drive setup")


# =========================================================================
#  Library config fixture
# =========================================================================


@pytest.fixture(scope="session")
def stress_library_conf(env: TestEnv) -> StressLibraryConf:
    """Read the library config from library.toml on the first stress-mode taped pod.

    This is the single source of truth for media types, tape counts, VID
    prefixes, capacities, and the logical library name — no duplication into
    test_params.toml is required or expected.
    """
    tapeds = _stress_tapeds(env)
    if not tapeds:
        pytest.skip("No stress-mode taped pods found")
    return tapeds[0].read_stress_library_conf()


# =========================================================================
#  Setup tests
# =========================================================================


def test_register_stress_media_types(
    cta_cli: CtaCliHost,
    stress_library_conf: StressLibraryConf,
) -> None:
    """Register every media type declared in library.toml."""
    for mt in stress_library_conf.media_types:
        cta_cli.exec(
            f"cta-admin mediatype add"
            f" --name {mt.name}"
            f" --capacity {mt.capacity_bytes}"
            f" --cartridge STRESS"
            f" --comment 'Virtual stress-test tape'"
        )


def test_register_stress_logical_library(
    cta_cli: CtaCliHost,
    stress_library_conf: StressLibraryConf,
) -> None:
    """Register the logical library that all stress drives and tapes belong to."""
    cta_cli.exec(
        f"cta-admin logicallibrary add"
        f" --name {stress_library_conf.name}"
        f" --comment 'Virtual logical library for stress drives'"
    )


def test_add_stress_tapes(
    cta_cli: CtaCliHost,
    stress_library_conf: StressLibraryConf,
) -> None:
    """Register all virtual tapes in the CTA catalogue."""
    for mt in stress_library_conf.media_types:
        for i in range(1, mt.count + 1):
            vid = _vid(mt.vid_prefix, i)
            cta_cli.exec(
                f"cta-admin tape add"
                f" --mediatype {mt.name}"
                f" --purchaseorder stress-order"
                f" --vendor stress-vendor"
                f" --logicallibrary {stress_library_conf.name}"
                f" --tapepool ctasystest"
                f" --vid {vid}"
                f" --full false"
                f" --comment 'Virtual stress tape'"
            )


def test_set_stress_drives_up(cta_cli: CtaCliHost, env: TestEnv) -> None:
    """Bring all stress drives up so the scheduler can dispatch work to them."""
    stress_pods = _stress_tapeds(env)
    expected_drives = {pod.drive_name for pod in stress_pods}

    # Wait for every stress drive to register itself in the scheduler DB.
    # taped registers drives on startup; the pod may still be initialising
    # when this test runs, so poll until all expected drives appear.
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
