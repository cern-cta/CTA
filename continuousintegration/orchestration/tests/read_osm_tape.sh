#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later




# Download OSM sample tape
dnf install -y git git-lfs
git lfs install --skip-repo
git clone https://gitlab.desy.de/mwai.karimi/osm-mhvtl.git /osm-mhvtl

device="$1"
# Load tape in a tapedrive
mtx -f /dev/smc status
mtx -f /dev/smc load 1 0
mtx -f /dev/smc status

# Get the device status where the tape is loaded and rewind it.
mt -f ${device} status
mt -f ${device} rewind

# The header files have 4 more bytes in the git file
truncate -s -4 /osm-mhvtl/L08033/L1
touch /osm-tape.img
dd if=/osm-mhvtl/L08033/L1 of=/osm-tape.img bs=32768
dd if=/osm-mhvtl/L08033/L2 of=/osm-tape.img bs=32768 seek=1
dd if=/osm-tape.img of=$device bs=32768 count=2
dd if=/osm-mhvtl/L08033/file1 of=$device bs=262144 count=202
dd if=/osm-mhvtl/L08033/file2 of=$device bs=262144 count=202

mt -f \$device rewind
