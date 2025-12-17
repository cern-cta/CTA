#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

source /etc/sysconfig/cta-taped
# for debugging
# echo "arguments passed: $@"
# eos:instance_name:eos_space
diskInstance=${1:-ctaeos}
spaceName=${2:-retrieve}

freespace=$(
  XrdSecSSSKT=$XrdSecSSSKT XrdSecPROTOCOL=$XrdSecPROTOCOL \
  eos -j root://$diskInstance space ls \
  | jq  -c ".result[] | select (.name==\"$spaceName\") | .sum.stat.statfs.freebytes | tonumber | floor"
)
if [[ $? -ne 0 ]]; then
  exit 1
fi

echo "{\"freeSpace\":$freespace}"
