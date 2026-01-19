#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -e

# This could be improved by:
# - Use a custom image to prevent doing installs here
# - Do the db initialization in an init container
# - Run the krb5kdc and kadmind in separate containers

# See: https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/6/html/managing_smart_cards/configuring_a_kerberos_5_server
# We only look in a single repo, as we want this script to run as fast as possible since the rest of the chart installations depend on this chart
microdnf install -y --disablerepo=* --enablerepo=baseos --setopt=install_weak_deps=0 krb5-server

echo "Initialising key distribution center... "
KRB5_DB_MASTER_KEY=$(openssl rand -base64 32)
kdb5_util create -s -r $KRB5_REALM -P $KRB5_DB_MASTER_KEY
kadmin.local -r $KRB5_REALM addprinc -pw $KRB5_ADMIN_PRINC_PWD $KRB5_ADMIN_PRINC_NAME/admin

krb5kdc
kadmind -nofork
