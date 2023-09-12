#!/bin/bash -e

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2022 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.

nlib=1

ndrive=3 # no more than 9 drives

if [ -n "$1" ]; then
  ndrive=$1
fi

ntape=7

freetapeslots=3


echo "VERSION: 5" > /etc/mhvtl/device.conf
echo >> /etc/mhvtl/device.conf

for ((lib=1; lib<=${nlib}; lib++)); do
  libid="${lib}0"
  cat <<EOF >> /etc/mhvtl/device.conf
Library: ${libid} CHANNEL: $(printf %.2d ${lib}) TARGET: 00 LUN: 00
 Vendor identification: STK
 Product identification: VLSTK${libid}
 Unit serial number: LIB${libid}
 NAA: 30:22:33:44:ab:$(printf %.2x ${lib}):00:00
 Compression: factor 1 enabled 1
 Compression type: lzo
 Home directory: /opt/mhvtl
 PERSIST: False
 Backoff: 400

EOF

  rm -f /etc/mhvtl/library_contents.${libid}

  for ((drive=1; drive<=${ndrive}; drive++)); do
    cat <<EOF >> /etc/mhvtl/device.conf
Drive: ${lib}${drive} CHANNEL: $(printf %.2d ${lib}) TARGET: $(printf %.2d ${drive}) LUN: 00
 Library ID: ${libid} Slot: $(printf %.2d ${drive})
 Vendor identification: STK
 Product identification: MHVTL
 Unit serial number: VDSTK${lib}${drive}
 NAA: 30:22:33:44:ab:$(printf %.2x ${lib}):$(printf %.2x ${drive}):00
 Compression: factor 1 enabled 1
 Compression type: lzo
 Backoff: 400

EOF

    echo "Drive ${drive}: VDSTK${lib}${drive}" >> /etc/mhvtl/library_contents.${libid}

  done

cat <<EOF >> /etc/mhvtl/library_contents.${libid}

Picker 1:

MAP 1:

# Slot 1 - ?, no gaps
# Slot N: [barcode]
# [barcode]
# a barcode is comprised of three fields: [Leading] [identifier] [Trailing]
# Leading "CLN" -- cleaning tape
# Leading "W" -- WORM tape
# Leading "NOBAR" -- will appear to have no barcode
# If the barcode is at least 8 character long, then the last two characters are Trailing
# Trailing "S3" - SDLT600
# Trailing "X4" - AIT-4
# Trailing "L1" - LTO 1, "L2" - LTO 2, "L3" - LTO 3, "L4" - LTO 4, "L5" - LTO 5
# Trailing "LT" - LTO 3 WORM, "LU" -  LTO 4 WORM, "LV" - LTO 5 WORM
# Trailing "L6" - LTO 6, "LW" - LTO 6 WORM
# Trailing "TA" - T10000+
# Trailing "TZ" - 9840A, "TY" - 9840B, "TX" - 9840C, "TW" - 9840D
# Trailing "TV" - 9940A, "TU" - 9940B
# Trailing "JA" - 3592+
# Trailing "JB" - 3592E05+
# Trailing "JC" - 3592E06+
# Trailing "JK" - 3592E07+
# Trailing "JW" - WORM 3592+
# Trailing "JX" - WORM 3592E05+ & 3592E06
# Trailing "JY" - WORM 3592E07+
# Trailing "D7" - DLT7000 media (DLT IV)
#
EOF

    for ((tape=1; tape<=${ntape}; tape++)); do
      echo "Slot ${tape}: V$(printf %.2d ${lib})$(printf %.3d ${tape})TA" >> /etc/mhvtl/library_contents.${libid}
    done

    for ((i=1;i<=${freetapeslots};i++)); do
      echo "Slot $((${ntape}+${i})):" >> /etc/mhvtl/library_contents.${libid}
    done

done
