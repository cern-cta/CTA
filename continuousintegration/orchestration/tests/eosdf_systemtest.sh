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

#default CI EOS instance
EOSINSTANCE=ctaeos

REPORT_DIRECTORY=/var/log

# would be eosdf but apparently this will not archive stuff to tape
EOSDF_BUFFER_BASEDIR=/eos/ctaeos/cta
EOSDF_BUFFER_URL=${EOSDF_BUFFER_BASEDIR}

FULL_EOSDF_BUFFER_URL=root://${EOSINSTANCE}/${EOSDF_BUFFER_BASEDIR}

# get some common useful helpers for krb5
. /root/client_helper.sh ## wait_for_archive is defined in this file

# get some common useful helpers for krb5
eospower_kdestroy
eospower_kinit

# Get kerberos credentials for user1
admin_kinit
admin_klist > /dev/null 2>&1 || die "Cannot get kerberos credentials for user ${USER}"

## All this is executed from within the client pod
# This should be idempotent as we will be called several times
if [[ $( admin_cta --json ds ls | jq '.[] | select(.name=="eosdfBuffer") | .name') != '"eosdfBuffer"' ]]; then
    admin_cta di add -n ${EOSINSTANCE} -m toto
    admin_cta dis add -n eosdfDiskInstanceSpace --di ${EOSINSTANCE} -u "eosSpace:default" -i 1 -m toto
    admin_cta ds add -n eosdfBuffer --di ${EOSINSTANCE} --dis eosdfDiskInstanceSpace -r ".*/eos/ctaeos/.*" -f 1 -s 20 -m toto # "root://${EOSINSTANCE}/${EOSDF_BUFFER_BASEDIR}"
else
    echo "Disk system eosdfBuffer alread defined. Ensuring very low free space requirements."
    admin_cta ds ch -n eosdfBuffer -f 1
fi
admin_cta ds ls

echo "Archiving a file: testfile1_eosdf"
echo
## Archive a file, so we can then retrieve it
TEST_FILE_NAME=testfile1_eosdf
echo "foo" > /root/${TEST_FILE_NAME}
echo
echo "Doing xrdcp of ${TEST_FILE_NAME} in the path root://${EOSINSTANCE}/${EOSDF_BUFFER_URL}/${TEST_FILE_NAME}"
xrdcp /root/${TEST_FILE_NAME} root://${EOSINSTANCE}/${EOSDF_BUFFER_URL}/${TEST_FILE_NAME}
wait_for_archive ${EOSINSTANCE} "${EOSDF_BUFFER_URL}/${TEST_FILE_NAME}"

echo "File ${TEST_FILE_NAME} archived to tape"

## Retrieve the file, this should have returned a script error, but still it will work because we don't want to prevent staging due to script missing or having insufficient permissions
REQUEST_ID=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_MGM_HOST} prepare -s ${EOSDF_BUFFER_URL}/testfile1_eosdf)
# Wait for the copy to appear on disk
wait_for_retrieve ${EOSINSTANCE} "${EOSDF_BUFFER_URL}/${TEST_FILE_NAME}"

echo
echo "File ${TEST_FILE_NAME} retrieved from disk"
echo

# remove the file so that it can be recreated in the next test run
echo "eos root://${EOSINSTANCE} rm ${EOSDF_BUFFER_URL}/${TEST_FILE_NAME}"
eos root://${EOSINSTANCE} rm ${EOSDF_BUFFER_URL}/${TEST_FILE_NAME}

# Remove the disk system so it doesn't interfere with other tests
admin_cta ds rm -n eosdfBuffer
admin_cta dis rm -n eosdfDiskInstanceSpace --di ${EOSINSTANCE}