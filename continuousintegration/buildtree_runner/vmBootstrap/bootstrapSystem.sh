#!/bin/bash

echo Initializing krb5.conf...
cp -fv krb5.conf /etc/krb5.conf

echo Adding user
adduser eric
passwd eric
cat >> /etc/sudoers << EOFsudoers
eric ALL=(ALL) NOPASSWD: ALL
EOFsudoers
sudo -u eric cp /vmBootstrap/gitScripts/.git-* ~eric/
cat /vmBootstrap/gitScripts/bash_profile.hook >> ~eric/.bash_profile
sudo -u eric cp /vmBootstrap/tigConf/tigrc ~eric/.tigrc

echo Installing minimal tools and tape tools
yum install -y git cmake rpm-build gcc gcc-c++ vim gdb cgdb strace ltrace screen tig kmod-mhvtl mhvtl-utils lsscsi mt-st mtx sg3_utils jq

sudo -u eric git config --global color.ui true
sudo -u eric git config --global user.email "Eric.Cano@cern.ch"
sudo -u eric git config --global user.name "Eric Cano"


