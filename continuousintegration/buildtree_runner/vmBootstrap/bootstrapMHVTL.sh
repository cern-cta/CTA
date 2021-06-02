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
