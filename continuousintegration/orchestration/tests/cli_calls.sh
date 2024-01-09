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


if [[ "${CLI_TARGET}" == "xrd" ]]; then
  archive='XRD_LOGLEVEL=Dump xrdcp - root://${EOSINSTANCE}/${EOS_DIR}/${subdir}/${subdir}TEST_FILE_NUM'

  retrieve='XRD_LOGLEVEL=Dump KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOSINSTANCE} prepare -s ${EOS_DIR}/${subdir}/${subdir}TEST_FILE_NAME?activity=T0Reprocess'

  evict='XrdSecPROTOCOL=krb5 KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0  xrdfs ${EOSINSTANCE} prepare -e ${EOS_DIR}/${subdir}/${subdir}TEST_FILE_NAME'

  delete='KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos root://${EOSINSTANCE} rm -Fr ${EOS_DIR} &'

elif [[ "${CLI_TARGET}" == "gfal2-root" ]]; then
  archive='XRD_LOGLEVEL=Dump xrdcp - root://${EOSINSTANCE}/${EOS_DIR}/${subdir}/${subdir}TEST_FILE_NUM'

  retrieve='XRD_LOGLEVEL=Dump KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 gfal-bringonline ${GFAL2_PROTOCOL}://${EOSINSTANCE}/${EOS_DIR}/${subdir}/${subdir}TEST_FILE_NAME'

  evict='XrdSecPROTOCOL=krb5 KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 gfal-evict ${GFAL2_PROTOCOL}://${EOSINSTANCE}/${EOS_DIR}/${subdir}/${subdir}TEST_FILE_NAME'

  delete='KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 gfal-rm -r ${GFAL2_PROTOCOL}://${EOSINSTANCE}/${EOS_DIR} 1>/dev/null &'

elif [[ "${CLI_TARGET}" == "gfal2-https" ]]; then
  SRC=$(mktemp)
  dd if=/dev/zero of=${SRC} bs=1k count=15 2>/dev/null
  archive='BEARER_TOKEN=${TOKEN} gfal-copy ${SRC} https://${EOSINSTANCE}:8444/${EOS_DIR}/${subdir}/${subdir}TEST_FILE_NUM'

  retrieve='BEARER_TOKEN=${TOKEN_EOSPOWER} gfal-bringonline https://${EOSINSTANCE}:8444/${EOS_DIR}/${subdir}/${subdir}TEST_FILE_NAME'

  evict='BEARER_TOKEN=${TOKEN_EOSPOWER} gfal-evict https://${EOSINSTANCE}:8444/${EOS_DIR}/${subdir}/${subdir}TEST_FILE_NAME'

  delete='BEARER_TOKEN=${TOKEN} gfal-rm -r https://${EOSINSTANCE}:8444/${EOS_DIR}'
fi
