#!/bin/bash

echo Initializing krb5.conf...
sudo cp -fv krb5.conf /etc/krb5.conf

CTAUSER=cta
if [[ ! -z $1 ]]; then
	CTAUSER=$1
fi
export CTAUSER

echo "Adding user '$CTAUSER'"
sudo adduser $CTAUSER
sudo passwd $CTAUSER

sudo -E bash -c 'cat >> /etc/sudoers << EOFsudoers
$CTAUSER ALL=(ALL) NOPASSWD: ALL
EOFsudoers'

CTAUSERHOME=`eval echo ~$CTAUSER`
export CTAUSERHOME
sudo chmod a+rx $CTAUSERHOME

sudo -u $CTAUSER cp gitScripts/.git-* $CTAUSERHOME/
sudo -E bash -c 'cat gitScripts/bash_profile.hook >> $CTAUSERHOME/.bash_profile'
sudo -u $CTAUSER cp tigConf/tigrc $CTAUSERHOME/.tigrc

echo Installing minimal tools and tape tools
sudo yum install -y git cmake rpm-build gcc gcc-c++ vim gdb cgdb strace ltrace screen tig lsscsi mt-st mtx sg3_utils jq psmisc mariadb-devel yum-plugin-versionlock krb5-workstation wget yum-utils

echo Installing Oracle instant client
sudo wget https://public-yum.oracle.com/RPM-GPG-KEY-oracle-ol7 -O /etc/pki/rpm-gpg/RPM-GPG-KEY-oracle
sudo wget https://yum.oracle.com/public-yum-ol7.repo -O /etc/yum.repos.d/public-yum-ol7.repo
sudo yum install -y oracle-instantclient19.3-basic.x86_64 oracle-instantclient19.3-devel.x86_64 --enablerepo=ol7_oracle_instantclient
