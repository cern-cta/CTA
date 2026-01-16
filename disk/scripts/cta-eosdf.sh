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
  xrdfs root://$diskInstance query space /?eos.space=${spaceName} \
  | sed -e 's/.*oss.space=//;s/&.*//'
)

if [[ $? -ne 0 ]]; then
  exit 1
fi

echo "{\"freeSpace\":$freespace}"
