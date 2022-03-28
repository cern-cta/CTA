#!/bin/bash

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

# Derived from https://gitlab.cern.ch/ci-tools/ci-web-deployer
# Merges Dockerfile and deploy-eos.sh
# From commit 15c6bdccbee313df5601ce8df34fc4455fe92905
#
# Copies provided artifacts and launch an additional hook

# April 2016 Borja Aparicio
# Receives:
# Environment variables
#   EOS_ACCOUNT_USERNAME
#   EOS_ACCOUNT_PASSWORD
#   CI_OUTPUT_DIR => default: public/
#   EOS_PATH
#   HOOK

#
#
# Produces:
#  Uploads to $EOS_PATH in the EOS namespace the files found in CI_WEBSITE_DIR

# Install what is not provided by the Dockerfile
yum install -y krb5-workstation rsync openssh-clients xrootd-client

# SSH will be used to connect to LXPLUS and there check if the EOS folder exists
# SSH not needed anymore as we use xrootd
ssh="/usr/bin/ssh"
if [ ! -x $ssh ]
then
        echo ERROR: $ssh not found
        exit 1
fi

# Authenticate user via Kerberos
kinit="/usr/bin/kinit"
if [ ! -x $kinit ]
then
        echo ERROR: $kinit not found
        exit 1
fi

kdestroy="/usr/bin/kdestroy"
if [ ! -x $kdestroy ]
then
        echo ERROR: $kdestroy not found
        exit 1
fi

# XROOTD client to copy files to EOS
xrdcp="/usr/bin/xrdcp"
if [ ! -x $xrdcp ]
then
        echo ERROR: $xrdcp not found
        exit 1
fi

# Validate input
: "${EOS_ACCOUNT_USERNAME:?EOS_ACCOUNT_USERNAME not provided}"
: "${EOS_ACCOUNT_PASSWORD:?EOS_ACCOUNT_PASSWORD not provided}"
: "${EOS_PATH:?EOS_PATH not provided}"

# Directory where the web site has been generated in the CI environment
# If not proviHOOK_ded by the user
if [ "X$CI_OUTPUT_DIR" == "X" ]
then
	CI_OUTPUT_DIR="public/"
fi

# Check the source directory exists
if [ ! -d $CI_OUTPUT_DIR ]
then
	echo "ERROR: Source directory $CI_OUTPUT_DIR doesn't exist"
	exit 1
fi

# Get credentials
echo "$EOS_ACCOUNT_PASSWORD" | $kinit $EOS_ACCOUNT_USERNAME@CERN.CH 2>&1 >/dev/null
if [ $? -ne 0 ]
then
	echo Failed to get Krb5 credentials for $EOS_ACCOUNT_USERNAME
        exit 1
fi

# Rely in xrootd to do the copy of files to EOS
$xrdcp --force --recursive $CI_OUTPUT_DIR/ root://eoshome.cern.ch/$EOS_PATH/ 2>&1 >/dev/null
if [ $? -ne 0 ]
then
    echo ERROR: Failed to copy files to $EOS_PATH via xrdcp
    exit 1
fi

# Run the provided HOOK
if [ -n "${HOOK}" ]
then
	$ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPITrustDNS=yes -o GSSAPIDelegateCredentials=yes $EOS_ACCOUNT_USERNAME@lxplus.cern.ch $HOOK 2>&1
if [ $? -ne 0 ]
then
	echo "Something wrong happened when running hook $HOOK on lxplus:"
        exit 1
fi
fi
echo "HOOK executed successfully"

# Rsync files with EOS
#$rsync --recursive --verbose -e "ssh -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPITrustDNS=yes -o GSSAPIDelegateCredentials=yes" $CI_OUTPUT_DIR/ $EOS_ACCOUNT_USERNAME@lxplus.cern.ch:$EOS_PATH/
#if [ $? -ne 0 ]
#then
#	echo ERROR: Rsync to \"$EOS_PATH\" via lxplus.cern.ch, failed
#        exit 1
#fi

# Destroy credentials
$kdestroy
if [ $? -ne 0 ]
then
	echo Krb5 credentials for $DFS_ACCOUNT_USERNAME have not been cleared up
fi

exit 0
