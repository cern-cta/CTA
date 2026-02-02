#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later



# Download EnstoreLarge sample tape
dnf install -y git git-lfs
git lfs install --skip-repo
git clone https://github.com/LTrestka/ens-mhvtl.git /ens-mhvtl

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

# Write EnstoreLarge label, header, payload, and trailer to tape.
label_block_size=80
label_blocks=2
touch /enstorelarge-tape.img
dd if=${ens_mhvtl_root}/enstorelarge/FL1587_f1/vol1_FL1587.bin of=/enstorelarge-tape.img bs=${label_block_size} count=1  # Build the first VOL1 label block.
truncate -s $((label_block_size * label_blocks)) /enstorelarge-tape.img  # Pad the label file to two blocks (OSM-style multi-block label).
dd if=/enstorelarge-tape.img of=$device bs=${label_block_size} count=${label_blocks}  # Write both label blocks to the tape drive.
mt -f ${device} status
dd if=${ens_mhvtl_root}/enstorelarge/FL1587_f1/fseq1_header.bin of=$device bs=262144  # Write the EnstoreLarge file header block to tape (256 KiB blocks).
mt -f ${device} status
dd if=${ens_mhvtl_root}/enstorelarge/FL1587_f1/fseq1_payload.bin of=$device bs=262144  # Write the EnstoreLarge payload to tape (256 KiB blocks).
mt -f ${device} status
dd if=${ens_mhvtl_root}/enstorelarge/FL1587_f1/fseq1_trailer.bin of=$device bs=262144  # Write the EnstoreLarge trailer block to tape (256 KiB blocks).

mt -f $device rewind
