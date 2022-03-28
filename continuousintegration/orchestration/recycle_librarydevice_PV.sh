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

LIBRARY_DIR="/opt/librarydevice"


kubectl get pv | grep \/claimlibrary\ | grep -v Bound |  while read line; do
  library_device=$(echo $line | awk '{print $1}')
  pv_status=$(echo $line | awk '{print $4}')
  echo "Deleting PV ${library_device} with status ${pv_status}"
  kubectl delete pv ${library_device} && \
  kubectl create -f /opt/kubernetes/CTA/library/resource/${library_device}_librarydevice_resource.yaml && \
  echo OK
done
