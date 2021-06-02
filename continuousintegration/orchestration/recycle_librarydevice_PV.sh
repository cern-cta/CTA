#!/bin/bash

# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2021 CERN
# @license        This program is free software: you can redistribute it and/or modify
#                 it under the terms of the GNU General Public License as published by
#                 the Free Software Foundation, either version 3 of the License, or
#                 (at your option) any later version.
#
#                 This program is distributed in the hope that it will be useful,
#                 but WITHOUT ANY WARRANTY; without even the implied warranty of
#                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#                 GNU General Public License for more details.
#
#                 You should have received a copy of the GNU General Public License
#                 along with this program.  If not, see <http://www.gnu.org/licenses/>.

LIBRARY_DIR="/opt/librarydevice"


kubectl get pv | grep \/claimlibrary\ | grep -v Bound |  while read line; do
  library_device=$(echo $line | awk '{print $1}')
  pv_status=$(echo $line | awk '{print $4}')
  echo "Deleting PV ${library_device} with status ${pv_status}"
  kubectl delete pv ${library_device} && \
  kubectl create -f /opt/kubernetes/CTA/library/resource/${library_device}_librarydevice_resource.yaml && \
  echo OK
done
