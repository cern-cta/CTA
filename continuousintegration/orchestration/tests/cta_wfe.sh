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


# Test every cta-admin command subcommand combination.
# The test is organized into differente sections, grouping the commands by semantics:
# 1 - Miscelaneous: Version
# 2 - Users/Actors: admin, virtual organization.
# 3 - Disk: disk instance, disk instance space, disk system.
# 4 - Tape: tape, tape file, tape pool, drive, logial library, media-type, recycle tape file.
# 5 - Workflow: activity mount rule, group mount rule, requester mount rule, archive route, mount policy, storage class.
# 6 - Requests: show queue, failed requests, repack.

set -a

# Echo a header for a test section.
# $1 -
test_header () {
  echo
  echo
  echo "####################################"
  echo "   Testing $1 related commands"
  echo "####################################"
}

# We can't compare directly the logs of all the commands
# in both frontends as time of modifications will be different
# so, just log the interesting fields for each test.
# $1 - command to ls.
log_command () {
    admin_cta --json "$1" ls ${ls_extra} |\
        jq -r 'del(.[] | .creationLog,.lastModificationLog | .time,.host)' >> "${log_file}"
}


# Echo a message into stdout and into the log file.
# $1 - Log message.
# $2 - Log file.
log_message () {
    echo "$1"
    echo "$1" >> "${log_file}"
}


# Compare the cta-admin command ls before and after the tests.
# They should be the same as the rm command is tested the last
# one over the added command.
test_assert () {
    end=$(admin_cta "${command}" ls ${ls_extra})
    if [[ "${start}" != "${end}" ]]; then
        log_message "ERROR. During ${command} test."
         exit 1
    fi
    echo
    echo
}

test_assert_false () {
  end=$(admin_cta "${command}" ls ${ls_extra})
  if [[ ${start} == "${end}" ]]; then
      log_message "ERROR. During ${command} test."
      exit 1
  fi
  echo
  echo
}


# Wrapper to test a command - sub command combination.
# $1 - Initial message to display and log
# $2 - command
# $3 - subcommand
# $4 - subcommand options
test_command () {
    # Echo initial Message
    log_message "$1" "${log_file}"

    # Execute command
    bash -c "admin_cta $2 $3 $4" >> "${log_file}" 2>&1 || exit 1

    # Log results
    log_command "$2"
}

# Wrapper to test a command - sub command combination that should fail
# $1 - Initial message to display and log
# $2 - command
# $3 - subcommand
# $4 - subcommand options
test_command_fails () {
    # Echo initial Message
    log_message "$1" "${log_file}"

    # Execute command
    bash -c "admin_cta $2 $3 $4" >> "${log_file}" 2>&1 && exit 1

    # Log results
    log_command "$2"
}

LS_TIMEOUT=15

# Run the test command function and check the result
# against the provided jq query and expected result value.
# $1-4: Used for test_command
# $5: jq query
# $6: expected value (tested with -eq)
# $7: log message for success/error.
test_and_check_cmd () {
  test_command "$1" "$2" "$3" "$4" || exit 1
  SECS=0

  while [[ ${SECS} -ne ${LS_TIMEOUT} ]]; do
      res=$(admin_cta --json "$2" ls ${ls_extra} | jq -r ".[] | $5" | wc -l)
      SECS=$(( ${SECS} + 1))
      test "${res}" -eq "$6" && log_message "Succes $7" && return 0
      echo "Waiting for command to complete (${SECS}s)..."
      sleep 1
  done

  log_message "Error while $7"
  exit 1
}

# Template:
# test_and_check_cmd "Init msg" "${command}" "subcmd" "subcmd_opts"\
#   'jq query after .[] |'\
#   "1" "End msg" "extra_ls_options" || exit 1


# $1 - Command full name (not long name).
# $2 - Command short name.
# $3 - Extra options for ls if needed.
test_start () {
  echo -e "\tTesting cta-wfe-test $@"
#  command=$1
#  extra=$2
#  start=$(wfa_cta "${command}")
#  log_command "${command}"
}


log_file="/root/log"
touch ${log_file}

. /root/client_helper.sh

EOSINSTANCE=ctaeos
TEST_DIR=/eos/ctaeos/cta/
TEST_FILE_NAME=testfile

admin_kdestroy &>/dev/null
admin_kinit &>/dev/null

eospower_kdestroy &>/dev/null
eospower_kinit &>/dev/null

admin_cta() {
  KRB5CCNAME=/tmp/${CTAADMIN_USER}/krb5cc_0 cta-admin "$@"
}


if admin_cta admin ls &>/dev/null; then
  echo "CTA Admin privileges ok, proceeding with test..."
else
  admin_cta admin ls > ${log_file}
  die "ERROR: Could not launch cta-admin command."
fi

wfe_cta() {
  log_message "Testing cta-wfe-test $@"
  KRB5CCNAME=/tmp/${CTAADMIN_USER}/krb5cc_0 cta-wfe-test "$@"
}


# Get drive names.
#IFS=' ' read -r -a dr_names <<< $(admin_cta --json dr ls | jq -r '.[] | select(.driveStatus=="UP") | .driveName')

#IFS=' ' read -r -a dr_names_down <<< $(admin_cta --json dr ls | jq -r '.[] | select(.driveStatus=="DOWN") | .driveName')

# Get tape names.
IFS=' ' read -r -a vids <<< $(admin_cta --json ta ls --all | jq -r '.[] | .vid')
#
## Get lls
#IFS=' ' read -r -a lls <<< $(admin_cta --json ll ls | jq -r '.[] | .name')


########################################
# Retrieve - reprot url ################
########################################
#test_start "retrieve" "--reportURL" "eosQuery://test"
log_message "Archiving 1 file."

echo "foo" > /root/testfile
xrdcp /root/testfile root://${EOSINSTANCE}/${TEST_DIR}${TEST_FILE_NAME}
sleep 15

log_message "Get tape."

# Get the actual tape we are working with.
for vid in "${vids[@]}"; do
  check=$(admin_cta --json tf ls -v "${vid}" | jq -r '.[]')
  if [[ -n ${check} ]]; then
    log_message "VID in use: ${vid}"
    break
  fi
done

ls_extra="-v ${vid}"
command="tf"

res=$(admin_cta --json "${command}" ls ${ls_extra} | jq -r '.[] | .tf | select(.copyNb==1) | .fSeq' | wc -l)
if test "${res}" -eq 1; then log_message "Succesfully listed tapefile"; else { log_message "Error listing log file"; exit 1; }; fi

a_id=$(admin_cta --json "${command}" ls ${ls_extra} | jq -r '.[] | .af | .archiveId')
#( test_command "Removing tape file" "${command}" "rm" "-v ${vid} -i ctaeos -I ${a_id} -r Test" && log_message "Error, removed the file." && exit 1; ) || log_message "Can't remove the file as there is only one copy of it. As expected."

log_message "Retrieve archive id: ${a_id}"

wfe_cta retrieve --instance ${EOSINSTANCE} --user ${CTAADMIN_USER} --group "eosusers" --dsturl "dst://url" --diskfilepath "dummy" --id ${a_id} --reportURL "eosQuery://test" 

#TODO: file is not retrieved (it is in the retrive queue???) 


#echo "Trigerring EOS retrieve workflow as poweruser1:powerusers (12001:1200)"
# We need the -s as we are staging the files from tape (see xrootd prepare definition)
#KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOSINSTANCE} prepare -s ${TEST_DIR}${TEST_FILE_NAME}

# Wait for the copy to appear on disk
wait_for_retrieve ${EOSINSTANCE} "${TEST_DIR}${TEST_FILE_NAME}"



eos root://${EOSINSTANCE} rm /eos/ctaeos/cta/testfile
sleep 1
#test_assert || exit 1



########################################
# Misc - v #############################
########################################
#test_header 'miscelaneous'
# Tool Version
#echo -e "\tTesting cta-admin v"
#admin_cta --json v >> ${log_file} 2>&1 || exit 1

#test_start "tape" "ta" "--all"

########################################
# Users - ad, vo #######################
########################################
#test_header 'user'

# Admin (ad)
#test_start "admin" "ad"
#test_and_check_cmd "Adding admin 'test_user'" "${command}" "add" "-u test_user -m 'Add test user'"\
#  'select(.user=="test_user" and .comment=="Add test user") | .user'\
#  "1" "adding admin 'test_user'" || exit 1
#test_and_check_cmd "Modifying admin 'test_user'" "${command}" "ch" "-u test_user -m 'Change test_user'"\
#  'select(.user=="test_user" and .comment=="Change test_user") | .user'\
#  "1" "changing admin 'test_user'" || exit 1
#test_command "Removing admin 'test_user'" "${command}" "rm" "-u test_user"
#test_assert || exit 1


log_message "OK. All tests passed."
exit 0

