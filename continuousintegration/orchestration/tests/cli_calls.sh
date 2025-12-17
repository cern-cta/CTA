#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


if [[ "${CLI_TARGET}" == "xrd" ]]; then
  archive='XRD_LOGLEVEL=Dump xrdcp - root://${EOS_MGM_HOST}/${EOS_DIR}/${subdir}/${subdir}TEST_FILE_NUM'

  retrieve='XRD_LOGLEVEL=Dump KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_MGM_HOST} prepare -s ${EOS_DIR}/${subdir}/${subdir}TEST_FILE_NAME?activity=T0Reprocess'

  evict_prefix='${EOS_DIR}/${subdir}/${subdir}'
  evict_count=40
  evict='XrdSecPROTOCOL=krb5 KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0  xrdfs ${EOS_MGM_HOST} prepare -e FILE_LIST'

  delete='KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos root://${EOS_MGM_HOST} rm -Fr ${EOS_DIR} &'

elif [[ "${CLI_TARGET}" == "gfal2-root" ]]; then
  archive='XRD_LOGLEVEL=Dump xrdcp - root://${EOS_MGM_HOST}/${EOS_DIR}/${subdir}/${subdir}TEST_FILE_NUM'

  retrieve='XRD_LOGLEVEL=Dump KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 gfal-bringonline root://${EOS_MGM_HOST}/${EOS_DIR}/${subdir}/${subdir}TEST_FILE_NAME'

  evict_prefix='root://${EOS_MGM_HOST}/${EOS_DIR}/${subdir}/${subdir}'
  evict_count=1
  evict='XrdSecPROTOCOL=krb5 KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 gfal-evict FILE_LIST'

  delete='KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 gfal-rm -r root://${EOS_MGM_HOST}/${EOS_DIR} 1>/dev/null &'

elif [[ "${CLI_TARGET}" == "gfal2-https" ]]; then
  SRC=$(mktemp)
  dd if=/dev/zero of=${SRC} bs=1k count=15 2>/dev/null
  archive='BEARER_TOKEN=${TOKEN_EOSUSER1} gfal-copy ${SRC} https://${EOS_MGM_HOST}:8443/${EOS_DIR}/${subdir}/${subdir}TEST_FILE_NUM 1\>/dev/null'

  retrieve='BEARER_TOKEN=${TOKEN_EOSPOWER1} gfal-bringonline https://${EOS_MGM_HOST}:8443/${EOS_DIR}/${subdir}/${subdir}TEST_FILE_NAME'

  evict_prefix='https://${EOS_MGM_HOST}:8443/${EOS_DIR}/${subdir}/${subdir}'
  evict_count=1
  evict='BEARER_TOKEN=${TOKEN_EOSPOWER1} gfal-evict FILE_LIST'

  delete='BEARER_TOKEN=${TOKEN_EOSUSER1} gfal-rm -r https://${EOS_MGM_HOST}:8443/${EOS_DIR} 1>/dev/null &'
fi
