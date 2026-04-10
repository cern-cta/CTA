# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

#####################################################################################################################
# Tests
#####################################################################################################################


def test_hosts_present_external_tape_formats(env):
    assert len(env.cta_taped) > 0
    assert len(env.cta_rmcd) > 0


def test_install_cta_integrationtests(env):
    env.cta_taped[0].exec("dnf -y install cta-integrationtests")


def test_read_osm_tape(env):
    drive_name = env.cta_taped[0].drive_name
    drive_device = env.cta_taped[0].drive_device
    print(f"Using drive: {drive_name}, device: {drive_device}")

    # Download OSM sample tape
    env.cta_rmcd[0].exec("dnf install -y git git-lfs")
    env.cta_rmcd[0].exec("git lfs install --skip-repo")
    env.cta_rmcd[0].exec("git clone https://gitlab.desy.de/mwai.karimi/osm-mhvtl.git /osm-mhvtl")

    # Load tape in a tapedrive
    env.cta_rmcd[0].exec("mtx -f /dev/smc status")
    env.cta_rmcd[0].exec("mtx -f /dev/smc load 1 0")
    env.cta_rmcd[0].exec("mtx -f /dev/smc status")

    # Get the device status where the tape is loaded and rewind it.
    env.cta_rmcd[0].exec(f"mt -f {drive_device} status")
    env.cta_rmcd[0].exec(f"mt -f {drive_device} rewind")

    # The header files have 4 more bytes in the git file
    env.cta_rmcd[0].exec("truncate -s -4 /osm-mhvtl/L08033/L1")
    env.cta_rmcd[0].exec("touch /osm-tape.img")
    env.cta_rmcd[0].exec("dd if=/osm-mhvtl/L08033/L1 of=/osm-tape.img bs=32768")
    env.cta_rmcd[0].exec("dd if=/osm-mhvtl/L08033/L2 of=/osm-tape.img bs=32768 seek=1")
    env.cta_rmcd[0].exec(f"dd if=/osm-tape.img of={drive_device} bs=32768 count=2")
    env.cta_rmcd[0].exec(f"dd if=/osm-mhvtl/L08033/file1 of={drive_device} bs=262144 count=202")
    env.cta_rmcd[0].exec(f"dd if=/osm-mhvtl/L08033/file2 of={drive_device} bs=262144 count=202")

    env.cta_rmcd[0].exec(f"mt -f {drive_device} rewind")


def test_osm_reader(env):
    drive_name = env.cta_taped[0].drive_name
    drive_device = env.cta_taped[0].drive_device
    print(f"Using drive: {drive_name}, device: {drive_device}")
    env.cta_taped[0].exec(f"cta-osmReaderTest {drive_name} {drive_device}")
