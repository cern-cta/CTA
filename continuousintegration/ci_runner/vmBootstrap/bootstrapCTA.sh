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

set -x

PUBLIC=true
CTA_VERSION=5
if [[ $1 == "cern" ]]; then
  PUBLIC=false
  echo Going to install from internal CERN repositories
fi

if [[ $2 == "4" ]]; then
  CTA_VERSION=4
  echo "Going to install CTA version 5"
fi

export CTA_VERSION

echo Enabling devtoolset-11
source /opt/rh/devtoolset-11/enable

echo Preparing CTA sources...
cd ~/CTA
git submodule update --init --recursive
git config submodule.recurse true

echo Creating source rpm
rm -rf ~/CTA-build-srpm
mkdir -p ~/CTA-build-srpm
(cd ~/CTA-build-srpm && cmake3 -DPackageOnly:Bool=true ../CTA; make cta_srpm)

# Admin yum repositiories
echo Installing repos
if [[ "$PUBLIC" == false ]] ; then
  for i in ~/CTA/continuousintegration/docker/ctafrontend/cc7/etc/yum.repos.d/*.repo; do
    sudo yum-config-manager --add-repo=$i
  done
else
  sudo wget https://public-yum.oracle.com/RPM-GPG-KEY-oracle-ol7 -O /etc/pki/rpm-gpg/RPM-GPG-KEY-oracle
  sudo wget https://download.ceph.com/keys/release.asc -O /etc/pki/rpm-gpg/RPM-ASC-KEY-ceph
  for i in ~/CTA/continuousintegration/docker/ctafrontend/cc7/etc/yum.repos.d-public/*.repo; do
    sudo yum-config-manager --add-repo=$i
  done
fi

# Set versionlock for xrootd and eos
sudo yum install -y yum-plugin-priorities
echo Adding versionlock for xrootd:
if [[ ${CTA_VERSION} -eq 4 ]];
then echo "Using XRootD version 4";
  sudo ~/CTA/continuousintegration/docker/ctafrontend/cc7/opt/run/bin/cta-versionlock --file ~/CTA/continuousintegration/docker/ctafrontend/cc7/etc/yum/pluginconf.d/versionlock.list config xrootd4;
  sudo yum-config-manager --disable cta-ci-xrootd5;
  sudo yum-config-manager --enable cta-ci-xroot;
else echo "Using XRootD version 5";
fi
sudo cp ~/CTA/continuousintegration/docker/ctafrontend/cc7/etc/yum/pluginconf.d/versionlock.list /etc/yum/pluginconf.d/versionlock.list

echo Creating source rpm
rm -rf ~/CTA-build-srpm
mkdir -p ~/CTA-build-srpm
(cd ~/CTA-build-srpm && cmake3 -DPackageOnly:Bool=true ../CTA; make cta_srpm)

echo Installing build dependencies
sudo yum-builddep -y ~/CTA-build-srpm/RPM/SRPMS/cta-*.src.rpm

echo Building CTA Rpms
rm -rf ~/CTA-build
mkdir -p ~/CTA-build
(cd ~/CTA-build && cmake3 ../CTA -DSKIP_UNIT_TESTS=1; make cta_rpm -j6)

echo CTA setup finished successfully
