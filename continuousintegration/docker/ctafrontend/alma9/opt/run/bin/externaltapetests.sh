#!/bin/bash

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

. /opt/run/bin/init_pod.sh

set -e

yum-config-manager --enable cta-artifacts
yum-config-manager --enable ceph
yum-config-manager --enable castor

# Install missing RPMs
yum -y install mt-st lsscsi sg3_utils cta-taped cta-tape-label cta-debuginfo ceph-common cta-systemtests

2>&1 cta-osmReaderTest ${DEVICE_NAME} ${DRIVE_DEVICE}
