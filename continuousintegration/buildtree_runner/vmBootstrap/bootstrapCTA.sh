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
if [[ $1 == "cern" ]]; then
  PUBLIC=false
  echo Going to install from internal CERN repositories
fi

echo Enabling devtoolset-8
source /opt/rh/devtoolset-8/enable

echo Preparing CTA sources...
cd ~/CTA
git submodule update --init --recursive

cat > ~/CTA/.git/hooks/post-checkout << EOFGitHook
#!/bin/sh
cd `git rev-parse --show-toplevel`
git submodule update --init --recursive
EOFGitHook

chmod +x ~/CTA/.git/hooks/post-checkout
cp ~/CTA/.git/hooks/post-checkout ~/CTA/.git/hooks/post-merge

echo Creating source rpm
rm -rf ~/CTA-build-srpm
mkdir -p ~/CTA-build-srpm
(cd ~/CTA-build-srpm && cmake3 -DPackageOnly:Bool=true ../CTA; make cta_srpm)

echo Installing repos

if [[ "$PUBLIC" == false ]] ; then
  for i in ~/CTA/continuousintegration/docker/ctafrontend/cc7/etc/yum.repos.d/*.repo; do
    sudo yum-config-manager --add-repo=$i
  done
else
  sudo wget https://public-yum.oracle.com/RPM-GPG-KEY-oracle-ol7 -O /etc/pki/rpm-gpg/RPM-GPG-KEY-oracle
  sudo wget https://git.ceph.com/release.asc -O /etc/pki/rpm-gpg/RPM-ASC-KEY-ceph
  for i in ~/CTA/continuousintegration/docker/ctafrontend/cc7/etc/yum.repos.d-public/*.repo; do
    sudo yum-config-manager --add-repo=$i
  done
fi

sudo yum install -y yum-plugin-priorities
echo Adding versionlock for xrootd:
sudo cp ~/CTA/continuousintegration/docker/ctafrontend/cc7/etc/yum/pluginconf.d/versionlock.list /etc/yum/pluginconf.d/versionlock.list

echo Installing build dependencies
sudo yum-builddep -y ~/CTA-build-srpm/RPM/SRPMS/cta-0-1.src.rpm

echo Building CTA
rm -rf ~/CTA-build
mkdir -p ~/CTA-build
(cd ~/CTA-build && cmake3 ../CTA; make -j 4)

echo CTA setup finished successfully
