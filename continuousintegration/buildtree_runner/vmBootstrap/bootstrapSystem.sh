#!/bin/bash

echo Initializing krb5.conf...
cp -fv krb5.conf /etc/krb5.conf

echo Adding user
adduser eric
passwd eric
cat >> /etc/sudoers << EOFsudoers
eric ALL=(ALL) NOPASSWD: ALL
EOFsudoers
chmod a+rx ~eric
sudo -u eric cp gitScripts/.git-* ~eric/
cat gitScripts/bash_profile.hook >> ~eric/.bash_profile
sudo -u eric cp tigConf/tigrc ~eric/.tigrc

echo Installing minimal tools and tape tools
yum install -y git cmake rpm-build gcc gcc-c++ vim gdb cgdb strace ltrace screen tig lsscsi mt-st mtx sg3_utils jq psmisc mariadb-devel yum-plugin-versionlock krb5-workstation wget yum-utils

echo Installing Oracle instant client
wget https://public-yum.oracle.com/RPM-GPG-KEY-oracle-ol7 -O /etc/pki/rpm-gpg/RPM-GPG-KEY-oracle
cd /etc/yum.repos.d
rm -f public-yum-ol7.repo
wget https://yum.oracle.com/public-yum-ol7.repo
yum-config-manager --enable ol7_oracle_instantclient
yum install -y oracle-instantclient19.3-basic.x86_64 oracle-instantclient19.3-devel.x86_64

(cd / ; sudo -u eric git config --global color.ui true)
(cd / ; sudo -u eric git config --global user.email "Eric.Cano@cern.ch")
(cd / ; sudo -u eric git config --global user.name "Eric Cano")

