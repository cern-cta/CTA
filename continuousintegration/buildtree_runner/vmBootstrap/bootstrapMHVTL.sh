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

PUBLIC=true
if [[ $1 == "cern" ]]; then
  PUBLIC=false
  echo Going to install from internal CERN repositories
fi

echo Installing mhvtl
if [[ "$PUBLIC" == false ]] ; then
  sudo yum install -y mhvtl-utils kmod-mhvtl --enablerepo=castor
else
  sudo yum install -y make gcc zlib-devel lzo-devel kernel kernel-tools kernel-devel kernel-headers
  git clone --depth 1 -b 1.6-3_release https://github.com/markh794/mhvtl.git
  cd mhvtl
  make
  sudo make install
  cd kernel
  make
  sudo make install

  sudo make_vtl_media -C /etc/mhvtl
  sudo systemctl start mhvtl.target
  sudo systemctl enable mhvtl.target
  echo "Please check the result of 'make install'. If it has failed, reboot and rerun this script"
fi

echo "mhvtl bootstrap finished"
