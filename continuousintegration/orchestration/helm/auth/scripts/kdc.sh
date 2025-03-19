#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2024 CERN
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

set -e

# This could be improved by:
# - Use a custom image to prevent doing installs here
# - Do the db initialization in an init container
# - Run the krb5kdc and kadmind in separate containers

# See: https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/6/html/managing_smart_cards/configuring_a_kerberos_5_server
# We only look in a single repo, as the CTA, XRootD and EOS repos are slowing things down significantly
dnf install -y --disablerepo=* --enablerepo=baseos krb5-server

echo "Initialising key distribution center... "
KRB5_DB_MASTER_KEY=$(openssl rand -base64 32)
kdb5_util create -s -r $KRB5_REALM -P $KRB5_DB_MASTER_KEY
kadmin.local -r $KRB5_REALM addprinc -pw $KRB5_ADMIN_PRINC_PWD $KRB5_ADMIN_PRINC_NAME/admin

krb5kdc
kadmind -nofork
