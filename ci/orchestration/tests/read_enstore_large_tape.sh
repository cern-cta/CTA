#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later



# Download EnstoreLarge sample tape
dnf install -y git git-lfs
git lfs install --skip-repo
git clone https://github.com/LTrestka/ens-mhvtl.git /ens-mhvtl

device="$1"
changer="${2:-/dev/smc}"

# Load tape in a tapedrive
mtx -f ${changer} status
mtx -f ${changer} load 3 0
mtx -f ${changer} status

# Get the device status where the tape is loaded and rewind it.
mt -f ${device} status
mt -f ${device} rewind

# Write EnstoreLarge label, header, payload, and trailer to tape.
dd if=/ens-mhvtl/enstorelarge/FL1587_f1/vol1_FL1587.bin of=$device bs=80
mt -f $device weof
dd if=/ens-mhvtl/enstorelarge/FL1587_f1/fseq1_header.bin of=$device bs=262144
mt -f $device weof
dd if=/ens-mhvtl/enstorelarge/FL1587_f1/fseq1_payload.bin of=$device bs=262144
mt -f $device weof
dd if=/ens-mhvtl/enstorelarge/FL1587_f1/fseq1_trailer.bin of=$device bs=262144
mt -f $device weof

mt -f $device rewind
