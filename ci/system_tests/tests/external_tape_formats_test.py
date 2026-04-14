# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import uuid
import pytest

#####################################################################################################################
# Helpers
#####################################################################################################################


@pytest.fixture(scope="session")
def cta_rmcd(env):
    return env.cta_rmcd[0]


@pytest.fixture(scope="session")
def cta_taped(env):
    return env.cta_taped[0]


@pytest.fixture(scope="session")
def drive_name(cta_taped):
    return cta_taped.drive_name


@pytest.fixture(scope="session")
def drive_device(cta_taped):
    return cta_taped.drive_device


#####################################################################################################################
# Tests
#####################################################################################################################


def test_hosts_present_external_tape_formats(env):
    assert len(env.cta_taped) > 0
    assert len(env.cta_rmcd) > 0


def test_install_required(cta_rmcd, cta_taped):
    cta_rmcd.exec("dnf install -y git git-lfs")
    cta_taped.exec("dnf -y install cta-integrationtests")


def test_load_tape(cta_rmcd):
    # Load tape in a tapedrive
    cta_rmcd.exec("mtx -f /dev/smc status")
    cta_rmcd.exec("mtx -f /dev/smc load 1 0")
    cta_rmcd.exec("mtx -f /dev/smc status")


def test_read_osm_tape(cta_rmcd, drive_device):
    osm_dir = "/osm_mhvtl_" + str(uuid.uuid4())[:8]

    # Download OSM sample tape
    cta_rmcd.exec("git lfs install --skip-repo")
    cta_rmcd.exec(f"git clone https://gitlab.desy.de/mwai.karimi/osm-mhvtl.git {osm_dir}")

    # Get the device status where the tape is loaded and rewind it.
    cta_rmcd.exec(f"mt -f {drive_device} status")
    cta_rmcd.exec(f"mt -f {drive_device} rewind")

    # The header files have 4 more bytes in the git file
    cta_rmcd.exec(f"truncate -s -4 /{osm_dir}/L08033/L1")
    cta_rmcd.exec(f"touch /{osm_dir}/osm-tape.img")
    cta_rmcd.exec(f"dd if=/{osm_dir}/L08033/L1 of=/{osm_dir}/osm-tape.img bs=32768")
    cta_rmcd.exec(f"dd if=/{osm_dir}/L08033/L2 of=/{osm_dir}/osm-tape.img bs=32768 seek=1")
    cta_rmcd.exec(f"dd if=/{osm_dir}/osm-tape.img of={drive_device} bs=32768 count=2")
    cta_rmcd.exec(f"dd if=/{osm_dir}/L08033/file1 of={drive_device} bs=262144 count=202")
    cta_rmcd.exec(f"dd if=/{osm_dir}/L08033/file2 of={drive_device} bs=262144 count=202")

    # Clean up the clone
    cta_rmcd.exec(f"rm -rf {osm_dir}")

    cta_rmcd.exec(f"mt -f {drive_device} rewind")


def test_osm_reader(cta_taped, drive_name, drive_device):
    print(f"Using drive: {drive_name}, device: {drive_device}")
    cta_taped.exec(f"cta-osmReaderTest {drive_name} {drive_device}")


def test_unload_tape(cta_rmcd):
    # Unload the tape again
    cta_rmcd.exec("mtx -f /dev/smc status")
    cta_rmcd.exec("mtx -f /dev/smc unload 1 0")
    cta_rmcd.exec("mtx -f /dev/smc status")
