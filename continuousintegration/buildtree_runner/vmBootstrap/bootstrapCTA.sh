#!/bin/bash

set -x

echo Getting CTA sources...
( cd ~ ; git clone https://:@gitlab.cern.ch:8443/cta/CTA.git)

echo Creating source rpm
mkdir -p ~/CTA-build-srpm
(cd ~/CTA-build-srpm && cmake -DPackageOnly:Bool=true ../CTA; make cta_srpm)

echo Installing repos
sudo yum-config-manager --add-repo=/vmBootstrap/eos.repo
sudo yum-config-manager --add-repo=/vmBootstrap/ceph-internal.repo
sudo yum-config-manager --add-repo=/vmBootstrap/cta-ci-xroot.repo
sudo yum-config-manager --add-repo=/vmBootstrap/castor.repo
sudo yum install -y yum-plugin-priorities

echo Adding versionlock for xrootd:
sudo bash -c "cat >> /etc/yum/pluginconf.d/versionlock.list << EOF
1:xrootd-libs-4.4.1-1.el7.x86_64
1:xrootd-devel-4.4.1-1.el7.x86_64
1:xrootd-client-libs-4.4.1-1.el7.x86_64
1:xrootd-server-libs-4.4.1-1.el7.x86_64
1:xrootd-client-devel-4.4.1-1.el7.x86_64
1:xrootd-server-devel-4.4.1-1.el7.x86_64
1:xrootd-private-devel-4.4.1-1.el7.noarch
EOF"

echo Installing build dependencies
sudo yum-builddep -y ~/CTA-build-srpm/RPM/SRPMS/cta-0-0.src.rpm --enablerepo=cernonly --nogpgcheck

echo Installing mhvtl
sudo yum install -y mhvtl-utils kmod-mhvtl mhvtl-utils --enablerepo=castor

echo Building CTA
mkdir -p ~/CTA-build
(cd ~/CTA-build && cmake ../CTA; make -j 4)

