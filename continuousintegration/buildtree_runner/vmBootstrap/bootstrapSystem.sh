#!/bin/bash -e

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

if [ ! -f /etc/krb5.conf ] || ! (grep default_realm /etc/krb5.conf | grep -qv '^ *#'); then
  echo Initializing krb5.conf...
  sudo cp -fv krb5.conf /etc/krb5.conf
fi

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
sudo yum install -y git cmake cmake3 rpm-build devtoolset-8 vim gdb cgdb strace ltrace screen tig lsscsi mt-st mtx sg3_utils jq psmisc yum-plugin-versionlock krb5-workstation wget yum-utils epel-release

echo "Getting CTA sources for $CTAUSER..."
sudo -u $CTAUSER bash -c 'cd ~ ; git clone https://gitlab.cern.ch/cta/CTA.git'

echo Tuning the system for EOS
sudo bash -c "echo net.ipv4.tcp_tw_reuse = 1 >> /etc/sysctl.d/00-eos.conf"
sudo sysctl -p

echo System bootstrap finished!
