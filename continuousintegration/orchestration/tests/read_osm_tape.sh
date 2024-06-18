#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2024 CERN
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

#

yum -y install git git-lfs

git lfs clone https://gitlab.desy.de/mwai.karimi/osm-mhvtl.git /osm-mhvtl


device=
# Load tape in a tapedrive
mtx -f /dev/smc status
mtx -f /dev/smc load 1 0
mtx -f /dev/smc status

# Get the device status where the tape is loaded and rewind it.
mt -f \$device status
mt -f \$device rewind

# The header files have 4 more bytes in the git file
truncate -s -4 /osm-mhvtl/L08033/L1
touch /osm-tape.img
dd if=/osm-mhvtl/L08033/L1 of=/osm-tape.img bs=32768
dd if=/osm-mhvtl/L08033/L2 of=/osm-tape.img bs=32768 seek=1
dd if=/osm-tape.img of=\$device bs=32768 count=2
dd if=/osm-mhvtl/L08033/file1 of=\$device bs=262144 count=202
dd if=/osm-mhvtl/L08033/file2 of=\$device bs=262144 count=202

mt -f \$device rewind
