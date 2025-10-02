#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
#
# SPDX-License-Identifier: GPL-3.0-or-later


# Archive some files.
dd if=/dev/urandom of=/root/test_act_xrd bs=15K count=1
xrdcp /root/test_act_xrd root://${EOS_MGM_HOST}/${EOS_DIR}/test_act_xrd

wait_for_archive ${EOS_MGM_HOST} ${EOS_DIR}/test_act_xrd

# Retrieve with activity
KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_MGM_HOST} prepare -s ${EOS_DIR}/${subdir}/test_act_xrd?activity=XRootD_Act

# Evict
XrdSecPROTOCOL=krb5 KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 xrdfs ${EOS_MGM_HOST} prepare -e ${EOS_DIR}/test_act_xrd
