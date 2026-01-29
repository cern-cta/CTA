#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

dnf install -y git git-lfs
git lfs install --skip-repo
git clone https://github.com/LTrestka/ens-mhvtl.git /ens-mhvtl

# Use preloaded Enstore sample tape files.
ens_mhvtl_root="/ens-mhvtl"
device="$1"
resolved_device=$(readlink -f "${device}" 2>/dev/null || echo "${device}")
drive_index=0
if [[ "${resolved_device}" =~ ([0-9]+)$ ]]; then
  drive_index="${BASH_REMATCH[1]}"
fi

# Load tape in a tapedrive
mtx -f /dev/smc status
mtx -f /dev/smc load 2 ${drive_index}
mtx -f /dev/smc status

# Get the device status where the tape is loaded and rewind it.
mt -f ${device} status
mt -f ${device} rewind

# Write Enstore label and payload to tape.
touch /enstore-tape.img
dd if=${ens_mhvtl_root}/enstore/FL1212_f1/vol1_FL1212.bin of=/enstore-tape.img bs=80
dd if=/enstore-tape.img of=$device bs=80
dd if=${ens_mhvtl_root}/enstore/FL1212_f1/fseq1_payload.bin of=$device bs=1048576

mt -f $device rewind
