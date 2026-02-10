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

CHANGER_DEVICE=$(lsscsi -g | awk '$2=="mediumx" {print $6; exit}')
echo "Checking changer device: ${CHANGER_DEVICE}"
ls -l ${CHANGER_DEVICE}

CHANGER_DEVICE=/dev/smc
echo "Using changer device: ${CHANGER_DEVICE}"
ls -l ${CHANGER_DEVICE}

echo "Using tape device: ${device} (resolved: ${resolved_device}), drive index: ${drive_index}"

# Load tape in a tapedrive
mtx -f ${CHANGER_DEVICE} load 1 ${drive_index}

# Get the device status where the tape is loaded and rewind it.
mt -f ${device} rewind

# Write Enstore label and payload to tape as stored in the repo.
dd if=${ens_mhvtl_root}/enstore/FL1212_f2/vol1_FL1212.bin of=$device bs=80
dd if=${ens_mhvtl_root}/enstore/FL1212_f2/fseq2_payload.bin of=$device bs=1048576


mt -f $device rewind
