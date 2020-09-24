#!/bin/bash -e

echo Initializing krb5.conf...
sudo cp -fv krb5.conf /etc/krb5.conf

CTAUSER=cta
if [[ ! -z $1 ]]; then
	CTAUSER=$1
fi
export CTAUSER

echo "Adding user '$CTAUSER'"
sudo adduser $CTAUSER || true
sudo passwd $CTAUSER || true

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
sudo yum install -y git cmake rpm-build gcc gcc-c++ vim gdb cgdb strace ltrace screen tig lsscsi mt-st mtx sg3_utils jq psmisc mariadb-devel yum-plugin-versionlock krb5-workstation wget yum-utils epel-release

echo "Getting CTA sources for $CTAUSER..."
sudo -u $CTAUSER bash -c 'cd ~ ; git clone https://gitlab.cern.ch/cta/CTA.git'

echo Tuning the system for EOS
sudo bash -c "echo net.ipv4.tcp_tw_reuse = 1 >> /etc/sysctl.d/00-eos.conf"
sudo sysctl -p

echo System bootstrap finished!
