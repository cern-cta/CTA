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
label_block_size=80
label_blocks=2
touch /enstore-tape.img
dd if=${ens_mhvtl_root}/enstore/FL1212_f1/vol1_FL1212.bin of=/enstore-tape.img bs=${label_block_size} count=1  # Build the first VOL1 label block.
truncate -s $((label_block_size * label_blocks)) /enstore-tape.img  # Pad the label file to two blocks (OSM-style multi-block label).
dd if=/enstore-tape.img of=$device bs=${label_block_size} count=${label_blocks}  # Write both label blocks to the tape drive.
mt -f ${device} status
dd if=${ens_mhvtl_root}/enstore/FL1212_f1/fseq1_payload.bin of=$device bs=1048576  # Stream the Enstore payload to tape in 1 MiB blocks.

mt -f $device rewind
