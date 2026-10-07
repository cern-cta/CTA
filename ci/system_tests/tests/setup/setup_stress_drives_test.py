# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Setup test for stress-drive mode.

Registers the virtual tape library in the CTA catalogue and prepares the
shared tmpfs directory used by StressDrive.  Must be run after
setup_cta_test.py (catalogue and admin users must already exist).

Applies only to deployments that include at least one taped pod configured
with DriveDevice = "stress://".  The test is automatically skipped when no
stress-mode drives are present.
"""

from concurrent.futures import ThreadPoolExecutor

import pytest

from system_tests.helpers.hosts import CtaCliHost, CtaTapedHost
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
#  Setup tests
# =========================================================================


def test_skip_if_no_stress_drives(env: TestEnv) -> None:
    """Skip the entire module when no stress-mode taped pods are present."""
    if not _stress_tapeds(env):
        pytest.skip("No stress-mode taped pods found; skipping stress-drive setup")


def test_register_stress_media_type(cta_cli: CtaCliHost, stress_tape_capacity_bytes: int) -> None:
    """Register the STRESS1T media type used for all virtual stress tapes."""
    cta_cli.exec(
        f"cta-admin mediatype add"
        f" --name STRESS1T"
        f" --capacity {stress_tape_capacity_bytes}"
        f" --cartridge STRESS"
        f" --comment 'Virtual stress-test tape'"
    )


def test_register_stress_logical_library(
    cta_cli: CtaCliHost,
    stress_logical_library: str,
) -> None:
    """Register the logical library that all stress drives and tapes belong to."""
    cta_cli.exec(
        f"cta-admin logicallibrary add"
        f" --name {stress_logical_library}"
        f" --comment 'Virtual logical library for stress drives'"
    )


def test_add_stress_tapes(
    cta_cli: CtaCliHost,
    stress_num_tapes: int,
    stress_vid_prefix: str,
    stress_logical_library: str,
) -> None:
    """Register all virtual tapes in the CTA catalogue."""
    for i in range(1, stress_num_tapes + 1):
        vid = _vid(stress_vid_prefix, i)
        cta_cli.exec(
            f"cta-admin tape add"
            f" --mediatype STRESS1T"
            f" --purchaseorder stress-order"
            f" --vendor stress-vendor"
            f" --logicallibrary {stress_logical_library}"
            f" --tapepool ctasystest"
            f" --vid {vid}"
            f" --full false"
            f" --comment 'Virtual stress tape'"
        )


def test_setup_stress_tmpfs(env: TestEnv, stress_base_dir: str) -> None:
    """Create tapes/ and drives/ subdirectories on every stress-mode taped pod.

    All CI taped pods are assumed to run on the same Kubernetes node and share
    the same hostPath volume, so calling mkdir -p from any one of them is
    sufficient.  We call it from all of them for resilience; mkdir -p is
    idempotent.
    """
    stress_pods = _stress_tapeds(env)
    with ThreadPoolExecutor(max_workers=len(stress_pods)) as pool:
        futures = [pool.submit(pod.setup_stress_tmpfs, stress_base_dir) for pod in stress_pods]
        for f in futures:
            f.result()


def test_set_stress_drives_up(cta_cli: CtaCliHost, env: TestEnv) -> None:
    """Bring all stress drives up so the scheduler can dispatch work to them."""
    stress_pods = _stress_tapeds(env)
    if not stress_pods:
        return

    for pod in stress_pods:
        drive_name = pod.drive_name
        print(f"Bringing stress drive {drive_name} up")
        cta_cli.exec(f"cta-admin drive up {drive_name} --reason 'stress-drive setup'")
