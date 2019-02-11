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
yum install -y git cmake rpm-build gcc gcc-c++ vim gdb cgdb strace ltrace screen tig lsscsi mt-st mtx sg3_utils jq psmisc mariadb-devel yum-plugin-versionlock

(cd / ; sudo -u eric git config --global color.ui true)
(cd / ; sudo -u eric git config --global user.email "Eric.Cano@cern.ch")
(cd / ; sudo -u eric git config --global user.name "Eric Cano")

