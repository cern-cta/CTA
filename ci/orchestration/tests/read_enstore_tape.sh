#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

dnf install -y git git-lfs
git lfs install --skip-repo
git clone https://github.com/LTrestka/ens-mhvtl.git /ens-mhvtl

# Use preloaded Enstore sample tape files.
ens_mhvtl_root="/ens-mhvtl"
device="$1"

# Load tape in a tapedrive
mtx -f /dev/smc status
mtx -f /dev/smc load 1 0
mtx -f /dev/smc status

# Get the device status where the tape is loaded and rewind it.
mt -f ${device} status
mt -f ${device} rewind
mt -f ${device} status

# Write Enstore label and payload to tape.
touch /enstore-tape.img
dd if=${ens_mhvtl_root}/enstore/FL1212_f1/vol1_FL1212.bin of=/enstore-tape.img bs=80
dd if=/enstore-tape.img of=$device bs=80 conv=fsync
mt -f ${device} status
dd if=${ens_mhvtl_root}/enstore/FL1212_f1/fseq1_payload.bin of=$device bs=1048576 conv=fsync

mt -f $device rewind
