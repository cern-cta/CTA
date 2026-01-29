#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later



# Download EnstoreLarge sample tape
dnf install -y git git-lfs
git lfs install --skip-repo
git clone https://github.com/LTrestka/ens-mhvtl.git /ens-mhvtl

ens_mhvtl_root="/ens-mhvtl"
device="$1"
resolved_device=$(readlink -f "${device}" 2>/dev/null || echo "${device}")
drive_index=0
if [[ "${resolved_device}" =~ ([0-9]+)$ ]]; then
  drive_index="${BASH_REMATCH[1]}"
fi

# Load tape in a tapedrive
mtx -f /dev/smc status
mtx -f /dev/smc load 3 ${drive_index}
mtx -f /dev/smc status

# Get the device status where the tape is loaded and rewind it.
mt -f ${device} status
mt -f ${device} rewind
mt -f ${device} status

# Write EnstoreLarge label, header, payload, and trailer to tape.
touch /enstorelarge-tape.img
dd if=${ens_mhvtl_root}/enstorelarge/FL1587_f1/vol1_FL1587.bin of=/enstorelarge-tape.img bs=80
dd if=/enstorelarge-tape.img of=$device bs=80 conv=fsync
mt -f ${device} status
dd if=${ens_mhvtl_root}/enstorelarge/FL1587_f1/fseq1_header.bin of=$device bs=262144 conv=fsync
mt -f ${device} status
dd if=${ens_mhvtl_root}/enstorelarge/FL1587_f1/fseq1_payload.bin of=$device bs=262144 conv=fsync
mt -f ${device} status
dd if=${ens_mhvtl_root}/enstorelarge/FL1587_f1/fseq1_trailer.bin of=$device bs=262144 conv=fsync

mt -f $device rewind
