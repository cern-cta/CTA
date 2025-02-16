#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2022-2024 CERN
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

COREFILE=$(ls /var/log/tmp/ctafrontend-*.core | head -n 1)

test -z ${COREFILE} && { echo "No core dumps found, exiting"; exit 1; }

echo "Processing corefile: ${COREFILE}"

dnf install -y xrootd-debuginfo cta-debuginfo gdb

cat <<EOF > /tmp/ctabt.gdb
file /usr/bin/xrootd
core ${COREFILE}
thread apply all bt
quit
EOF

gdb -x /tmp/ctabt.gdb > ${COREFILE}.bt

echo "Backtrace available in ${COREFILE}.bt"
exit 0
