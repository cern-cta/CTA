#!/usr/bin/env sh

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2023 CERN
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
  archive='XRD_LOGLEVEL=Dump xrdcp - root://${EOSINSTANCE}/${EOS_DIR}/${subdir}/TEST_FILE_NUM 2>${ERROR_DIR}/${subdir}TEST_FILE_NUM'

  retrieve='XRD_LOGLEVEL=Dump KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOSINSTANCE} prepare -s ${EOS_DIR}/${subdir}/TEST_FILE_NAME?activity=T0Reprocess 2>${ERROR_DIR}/${subdir}RETRIEVE_TEST_FILE_NAME'

  evict='XrdSecPROTOCOL=krb5 KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0  xrdfs ${EOSINSTANCE} prepare -e ${EOS_DIR}/${subdir}/TEST_FILE_NAME > /dev/null'

  delete='KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos root://${EOSINSTANCE} rm -Fr ${EOS_DIR} &'

elif [[ "${CLI_TARGET}" == "gfal2" ]]; then
  archive='XRD_LOGLEVEL=Dump xrdcp - root://${EOSINSTANCE}/${EOS_DIR}/${subdir}/TEST_FILE_NUM 2>${ERROR_DIR}/${subdir}TEST_FILE_NUM'

  retrieve='XRD_LOGLEVEL=Dump KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 gfal-bringonline ${GFAL2_PROTOCOL}://${EOSINSTANCE}/${EOS_DIR}/${subdir}/TEST_FILE_NAME 2>${ERROR_DIR}/${subdir}RETRIEVE_TEST_FILE_NAME'

  evict='XrdSecPROTOCOL=krb5 KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 gfal-evict ${GFAL2_PROTOCOL}://${EOSINSTANCE}/${EOS_DIR}/${subdir}/TEST_FILE_NAME'

  delete='KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 gfal-rm -r ${GFAL2_PROTOCOL}://${EOSINSTANCE}/${EOS_DIR} 1>/dev/null &'

fi
