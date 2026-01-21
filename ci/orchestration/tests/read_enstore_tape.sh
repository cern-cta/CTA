#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later



# Download Enstore sample tape
dnf install -y git git-lfs
git lfs install --skip-repo
git clone https://github.com/LTrestka/ens-mhvtl.git /ens-mhvtl

device="$1"
changer="${2:-/dev/smc}"

# Load tape in a tapedrive
mtx -f ${changer} status
mtx -f ${changer} load 2 0
mtx -f ${changer} status

# Get the device status where the tape is loaded and rewind it.
mt -f ${device} status
mt -f ${device} rewind

# Write Enstore label and payload to tape.
dd if=/ens-mhvtl/enstore/FL1212_f1/vol1_FL1212.bin of=$device bs=80
mt -f $device weof
dd if=/ens-mhvtl/enstore/FL1212_f1/fseq1_payload.bin of=$device bs=1048576
mt -f $device weof

mt -f $device rewind
