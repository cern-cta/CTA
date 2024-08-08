#!/bin/bash

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
  echo -e "\tTesting cta-admin $1 ($2)"
  command=$2
  ls_extra=$3
  start=$(admin_cta "${command}" ls ${ls_extra})
  log_command "${command}"
}


log_file="/root/log"
touch ${log_file}

. /root/client_helper.sh

EOSINSTANCE=ctaeos

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

readarray -t dr_names <<< $(admin_cta --json dr ls | jq -r '.[] | select(.driveStatus=="UP") | .driveName')
readarray -t dr_names_down <<< $(admin_cta --json dr ls | jq -r '.[] | select(.driveStatus=="DOWN") | .driveName')
readarray -t vids <<< $(admin_cta --json ta ls --all | jq -r '.[] | .vid')
readarray -t lls <<< $(admin_cta --json ll ls | jq -r '.[] | .name')

########################################
# Misc - v #############################
########################################
test_header 'miscelaneous'
# Tool Version
echo -e "\tTesting cta-admin v"
admin_cta --json v >> ${log_file} 2>&1 || exit 1

test_start "tape" "ta" "--all"

########################################
# Users - ad, vo #######################
########################################
test_header 'user'

# Admin (ad)
test_start "admin" "ad"
test_and_check_cmd "Adding admin 'test_user'" "${command}" "add" "-u test_user -m 'Add test user'"\
  'select(.user=="test_user" and .comment=="Add test user") | .user'\
  "1" "adding admin 'test_user'" || exit 1
test_and_check_cmd "Modifying admin 'test_user'" "${command}" "ch" "-u test_user -m 'Change test_user'"\
  'select(.user=="test_user" and .comment=="Change test_user") | .user'\
  "1" "changing admin 'test_user'" || exit 1
test_command "Removing admin 'test_user'" "${command}" "rm" "-u test_user"
test_assert || exit 1


# Virtual Organizations (vo)
test_start "virtual organization" "vo"
test_and_check_cmd "Adding vo 'vo_cta'" "${command}" "add" "--vo 'vo_cta' --rmd 1 --wmd 1 --di 'ctaeos' -m 'Add vo_cta'"\
  'select(.name=="vo_cta" and .writeMaxDrives=="1" and .readMaxDrives=="1" and .diskinstance=="ctaeos" and .comment=="Add vo_cta" and .isRepackVo==false) | .name'\
  "1" "adding vo 'vo_cta'" || exit 1
test_and_check_cmd "Changing 'vo_cta' rmd and mfs" "${command}" "ch" "--vo 'vo_cta' --wmd 2 --mfs 100 --isrepackvo false"\
  'select(.name=="vo_cta" and .writeMaxDrives=="2" and .maxFileSize=="100" and .isRepackVo==false) | .name'\
  "1" "changing 'vo_cta'" || exit 1
test_command "Removing vo 'vo_cta'" "${command}" "rm" "--vo 'vo_cta'" || exit 1
test_assert || exit 1


########################################
# Disk - di, dis, ds ###################
########################################
test_header 'disk'

# Disk Instance (di)
test_start "disk instance" "di"
test_and_check_cmd "Adding di 'ctaeos2'" "${command}" "add" "-n ctaeos2 -m 'Add di ctaeos2'"\
  'select(.name=="ctaeos2" and .comment=="Add di ctaeos2") | .name'\
  "1" "adding disk instance 'ctaeos2'" || exit 1
test_and_check_cmd "Changing di 'ctaeos2'" "${command}" "ch"  "-n ctaeos2 -m 'Change di ctaeos2'"\
 'select(.name=="ctaeos2" and .comment=="Change di ctaeos2") | .name'\
 "1" "changing disk instance 'ctaeos2'" || exit 1
test_command "Removing di 'ctaeos2'" "${command}" "rm"  "-n ctaeos2" || exit 1
test_assert || exit 1


# Disk Instance Space (dis)
test_start "disk instance space" "dis" || exit 1
test_and_check_cmd "Adding dis 'ctaeos_dis'" "${command}" "add" "-n ctaeos_dis --di ${EOSINSTANCE} -i 10 -u eosSpace:default -m 'Add dis ctaeos_dis'"\
  'select(.name=="ctaeos_dis" and .refreshInterval=="10" and .freeSpaceQueryUrl=="eosSpace:default") | .comment'\
  "1" "adding disk instance space 'ctaeos_dis'" || exit 1
test_and_check_cmd "Changing dis 'ctaeos_dis'" "${command}" "ch" "-n ctaeos_dis --di ${EOSINSTANCE} -i 100 -m 'Change dis ctaeos_dis'"\
  'select(.name=="ctaeos_dis" and .refreshInterval=="100" and .comment=="Change dis ctaeos_dis") | .comment'\
  "1" "changing disk instance space 'ctaeos_dis'" || exit 1
test_command "Deleting dis 'ctaeos_dis'" "${command}" "rm" "-n ctaeos_dis --di ${EOSINSTANCE}" || exit 1
test_assert || exit 1


# Disk System (ds)
test_start "disk system" "ds"

admin_cta dis add  -n ctaeos_dis --di ${EOSINSTANCE} -i 10 -u eosSpace:default -m 'Add dis ctaeos_dis' || { log_message "Error creating disk instance space for disk system test."; exit 1; }

test_and_check_cmd "Adding ds 'ctaeos_ds'" "${command}" "add" "-n ctaeos_ds --di ${EOSINSTANCE} --dis ctaeos_dis -r root://${EOSINSTANCE}//eos/ctaeos/cta/ -f 10000 -s 10 -m 'Adding ds ctaeos_ds'"\
  'select(.name=="ctaeos_ds" and .targetedFreeSpace=="10000" and .sleepTime=="10" and .comment=="Adding ds ctaeos_ds") | .name '\
  "1" "adding disk system 'ctaeos_ds'" || exit 1
test_and_check_cmd "Changing ds 'ctaeos_ds'" "${command}" "ch" "-n ctaeos_ds -s 100"\
  'select(.name=="ctaeos_ds" and .sleepTime=="100") | .name'\
  "1" "changing disk system ctaeos_ds" || exit 1
test_command "Removing ds 'ctaeos_ds'" "${command}" "rm" "-n ctaeos_ds" || exit 1
test_assert || exit 1

admin_cta dis rm -n ctaeos_dis --di ${EOSINSTANCE}

########################################
# Tape - ta, tf, tp, dr, ll, mt, rtf ###
########################################
test_header 'tape'


# Tape (ta)
test_start "tape" "ta" "--all"
admin_cta pl add --name phys1 --ma man --mo mod --npcs 3 --npds 4
admin_cta ll ch --name ${lls[1]} --pl phys1
# Set added tape to full so we can test reclaim.
test_and_check_cmd "Adding tape 'V01008'" "${command}" "add" "-v V01008 --mt T10K500G --ve vendor -l ${lls[1]} -t ctasystest -f true --purchaseorder order1"\
  "select(.vid==\"V01008\" and .mediaType==\"T10K500G\" and .logicalLibrary==\"${lls[1]}\" and .physicalLibrary==\"phys1\" and .full==true and .purchaseOrder==\"order1\") | .vid"\
  "1" "adding tape 'V01008'" || exit 1
test_and_check_cmd "Reclaiming tape 'V01008'" "${command}" "reclaim" "-v V01008"\
  "select(.vid==\"V01008\" and .mediaType==\"T10K500G\" and .logicalLibrary==\"${lls[1]}\" and .full==false) | .vid"\
  "1" "reclaiming tape 'V01008'" || exit 1
test_and_check_cmd "Changing tape V01008 state to REPACKING" "${command}" "ch" "-v V01008 -s 'REPACKING' -r 'Test admin-cta ta ch'"\
  "select(.vid==\"V01008\" and .mediaType==\"T10K500G\" and .logicalLibrary==\"${lls[1]}\" and .state==\"REPACKING\") | .vid"\
  "1" "changing tape V01008 state" || exit 1
test_and_check_cmd "Changing tape V01008 order to order2" "${command}" "ch" "-v V01008 --purchaseorder 'order2' -r 'Test admin-cta ta ch'"\
  "select(.vid==\"V01008\" and .purchaseOrder==\"order2\") | .vid"\
  "1" "changing tape V01008 order" || exit 1
test_command "Removing tape V01008" "${command}" "rm" "-v V01008" || exit 1
admin_cta ll ch --name ${lls[1]} --pl ""
admin_cta pl rm --name phys1
test_assert || exit 1

# Tape File (tf)
test_start "tape file" "tf" "-v ${vids[0]}"

log_message "Archiving 1 file."

echo "foo" > /root/testfile
xrdcp /root/testfile root://${EOSINSTANCE}//eos/ctaeos/cta/testfile
sleep 15

# Get the actual tape we are working with.
for vid in "${vids[@]}"; do
  check=$(admin_cta --json tf ls -v "${vid}" | jq -r '.[]')
  if [[ -n ${check} ]]; then
    log_message "VID in use: ${vid}"
    break
  fi
done

ls_extra="-v ${vid}"

res=$(admin_cta --json "${command}" ls ${ls_extra} | jq -r '.[] | .tf | select(.copyNb==1) | .fSeq' | wc -l)
if test "${res}" -eq 1; then log_message "Succesfully listed tapefile"; else { log_message "Error listing log file"; exit 1; }; fi

a_id=$(admin_cta --json "${command}" ls ${ls_extra} | jq -r '.[] | .af | .archiveId')
( test_command "Removing tape file" "${command}" "rm" "-v ${vid} -i ctaeos -I ${a_id} -r Test" && log_message "Error, removed the file." && exit 1; ) || log_message "Can't remove the file as there is only one copy of it. As expected."

eos root://${EOSINSTANCE} rm /eos/ctaeos/cta/testfile
sleep 1
test_assert || exit 1


# Tape Pool (tp)
test_start "tape pool" "tp"
test_and_check_cmd "Adding tape pool 'cta_admin_test'" "${command}" "add" "-n 'cta_admin_test' --vo vo -p 0 -e false -m 'Test tp cmd'"\
  'select(.name=="cta_admin_test" and .vo=="vo" and .numPartialTapes=="0" and .encrypt==false and .comment=="Test tp cmd") | .vo'\
  "1" "adding tape pool 'cta_admin_test'" || exit 1
test_and_check_cmd "Changing tape pool 'cta_admin_test'" "${command}" "ch" "-n 'cta_admin_test' -e true"\
  'select(.name=="cta_admin_test" and .encrypt==true) | .vo'\
  "1" "changing tape pool 'cta_admin_test'" || exit 1
test_command "Removing tape pool 'cta_admin_test'" "${command}" "rm" "-n 'cta_admin_test'" || exit 1
test_assert|| exit 1


# Drive (dr)
test_start "drive" "dr"
admin_cta pl add --name phys1 --ma man --mo mod --npcs 3 --npds 4
admin_cta ll ch --name ${lls[1]} --pl phys1
test_and_check_cmd "Setting drive '${dr_names_down[0]}' to UP" "${command}" "up" "${dr_names_down[0]} -r 'cta-admin systest up'"\
  "select(.logicalLibrary==\"${dr_names_down[0]}\" and .physicalLibrary==\"phys1\" and .driveName==\"${dr_names_down[0]}\" and .driveStatus==\"UP\" and .reason==\"cta-admin systest up\") | .driveName"\
  "1" "setting drive \'${dr_names_down[0]}\' to up state"|| exit 1
test_and_check_cmd "Setting drive '${dr_names_down[0]}' to DOWN" "${command}" "down" "${dr_names_down[0]} -r 'cta-admin systest down'"\
  "select(.logicalLibrary==\"${dr_names_down[0]}\" and .driveName==\"${dr_names_down[0]}\" and .driveStatus==\"DOWN\" and .reason==\"cta-admin systest down\") | .driveName"\
  "1" "setting drive \'${dr_names_down[0]}\' to down state" || exit 1
test_and_check_cmd "Changing drive \'${dr_names_down[0]}\' message" "${command}" "ch" "${dr_names_down[0]} -m 'cta-admin test ch'"\
  "select(.logicalLibrary==\"${dr_names_down[0]}\" and .comment==\"cta-admin test ch\") | .driveName"\
  "1" "changing drive \'${dr_names_down[0]}\' comment" || exit 1
test_command "Removing drive \'${dr_names_down[0]}\'" "${command}" "rm" "${dr_names_down[0]}" || exit 1
admin_cta ll ch --name ${lls[1]} --pl ""
admin_cta pl rm --name phys1
test_assert_false || exit 1



# Notify drive has been deleted.
log_message "Notify drive has been deleted."
echo "${dr_names_down[0]}" > /root/deleted.txt


# Physical Library (ll)
test_start "physical library" "pl"
test_and_check_cmd "Adding physical library 'cta_adm_systest'" "${command}" "add" "--name 'cta_adm_systest' --manufacturer 'manA' --model 'modA' --location 'locA'\\
   --type 'typeA' --guiurl 'urlA' --webcamurl 'urlA' --nbphysicalcartridgeslots 4 --nbavailablecartridgeslots 3 --nbphysicaldriveslots 2 --comment 'commentA'"\
  'select(.name=="cta_adm_systest" and .manufacturer=="manA" and .model=="modA" and .type=="typeA" and .guiUrl=="urlA" and .webcamUrl=="urlA" and .location=="locA" and .nbPhysicalCartridgeSlots=="4" and .nbAvailableCartridgeSlots=="3" and .nbPhysicalDriveSlots=="2" and .comment=="commentA") | .name'\
  "1" "adding physical library 'cta_adm_systest'"|| exit 1
echo ${command}
test_and_check_cmd "Modifying physical library 'cta_adm_systest'" "${command}" "ch" "--name 'cta_adm_systest' --location 'locB'\\
   --guiurl 'urlB' --webcamurl 'urlB' --nbphysicalcartridgeslots 4 --nbavailablecartridgeslots 3 --nbphysicaldriveslots 2 --comment 'commentB'"\
  'select(.name=="cta_adm_systest" and .guiUrl=="urlB" and .webcamUrl=="urlB" and .location=="locB" and .nbPhysicalCartridgeSlots=="4" and .nbAvailableCartridgeSlots=="3" and .nbPhysicalDriveSlots=="2" and .comment=="commentB") | .name'\
  "1" "adding physical library 'cta_adm_systest'"|| exit 1
test_command_fails "Adding a duplicate physical library 'CTA_ADM_SYSTEST' should fail"\
                   "${command}"\
                   "add"\
                   "--name 'cta_adm_systest' --manufacturer 'manA' --model 'modA' --location 'locA' --type 'typeA' --guiurl 'urlA' --webcamurl 'urlA' --nbphysicalcartridgeslots 4 --nbavailablecartridgeslots 3 --nbphysicaldriveslots 2 --comment 'commentA'"\
                   || exit 1
test_command "Removing physical library 'cta_adm_systest'" "${command}" "rm" "--name 'cta_adm_systest'"
test_assert || exit 1

# Logical Library (ll)
test_start "logical library" "ll"
admin_cta pl add --name phys1 --ma man --mo mod --npcs 3 --npds 4
admin_cta pl add --name phys2 --ma man --mo mod --npcs 3 --npds 4
test_and_check_cmd "Adding logical library 'cta_adm_systest'" "${command}" "add" "-n 'cta_adm_systest' -d false --pl phys1 -m 'cta-admin systest add'"\
  'select(.name=="cta_adm_systest" and .isDisabled==false and .physicalLibrary=="phys1" and .comment=="cta-admin systest add") | .name'\
  "1" "adding logical library 'cta_adm_systest'"|| exit 1
test_and_check_cmd "Adding logical library 'cta_adm_systest'" "${command}" "add" "-n 'cta_adm_systest2' -d false --pl phys1 -m 'cta-admin systest add'"\
  'select(.name=="cta_adm_systest" and .isDisabled==false and .physicalLibrary=="phys1" and .comment=="cta-admin systest add") | .name'\
  "1" "adding logical library 'cta_adm_systest'"|| exit 1
test_and_check_cmd "Changing logical library 'cta_adm_systest' to disabled" "${command}" "ch" "-n 'cta_adm_systest' -d true --pl phys2 --dr 'cta-admin systest ch'"\
  'select(.name=="cta_adm_systest" and .isDisabled==true and .physicalLibrary=="phys2" and .disabledReason=="cta-admin systest ch") | .name'\
  "1" "changing logical library 'cta_adm_systest'"|| exit 1
test_command "Removing logical library 'cta_adm_systest'" "${command}" "rm" "-n cta_adm_systest" || exit 1
test_command "Removing logical library 'cta_adm_systest'" "${command}" "rm" "-n cta_adm_systest2" || exit 1
admin_cta pl rm --name phys1
admin_cta pl rm --name phys2
test_assert || exit 1

# Media Type (mt)
test_start "media type" "mt"
test_and_check_cmd "Adding media type 'CTAADMSYSTEST'" "${command}" "add" "-n 'CTAADMSYSTEST' -t 12345C -c 5000000000000 -p 50 -m 'cta-admin systest add cartridge'"\
  'select(.name=="CTAADMSYSTEST" and .cartridge=="12345C" and .capacity=="5000000000000" and .primaryDensityCode=="50" and .comment=="cta-admin systest add cartridge") | .name'\
  "1" "adding media type 'CTAADMSYSTEST'" || exit 1
test_and_check_cmd "Changing media type 'CTAADMSYSTEST'" "${command}" "ch" "-n 'CTAADMSYSTEST' -w 10 -m 'cta-admin systest ch'"\
  'select(.name=="CTAADMSYSTEST" and .numberOfWraps=="10" and .comment=="cta-admin systest ch") | .name '\
  "1" "changing 'CTAADMSYSTEST' media type" || exit 1
test_command "Removing media type 'CTAADMSYSTEST'" "${command}" "rm" "-n 'CTAADMSYSTEST'" || exit 1
test_assert || exit 1


# Recycle Tape File (rtf) (list tape files in the recycle log)
test_start "recycle tape file" "rtf" "-v ${vid}"
res=$(admin_cta --json "${command}" ls ${ls_extra} | jq -r ".[] | select(.vid==\"${vid}\") | .vid" | wc -l )
if test "${res}" -eq 1; then log_message "Succesfully listed deleted file."; else { log_message "Error, no file was deleted"; exit 1; }; fi


########################################
# Workflow - amr, gmr, rmr, ar, mp, sc #
########################################
test_header 'workflow'

# Activity Mount Rule (amr)
test_start "activity mount rule" "amr"
test_and_check_cmd "Adding activity mount rule '^T1Reprocess'" "${command}" "add" "-i ctaeos -n powerusers --ar ^T1Reprocess -u repack_ctasystest -m 'cta-admin systest add amr'"\
  'select(.diskInstance=="ctaeos" and .activityMountRule=="powerusers" and .activityRegex=="^T1Reprocess" and .mountPolicy=="repack_ctasystest" and .comment=="cta-admin systest add amr") | .comment'\
  "1" "adding activity mount rule" || exit 1
test_and_check_cmd "Changing activity mount rule" "${command}" "ch" "-i ctaeos -n powerusers --ar ^T1Reprocess -m 'cta-admin systest ch amr'"\
  'select(.diskInstance=="ctaeos" and .activityMountRule=="powerusers" and .activityRegex=="^T1Reprocess" and .comment=="cta-admin systest ch amr") | .comment '\
  "1" "changign activity mount rule regex"|| exit 1
test_command "Removing activity mount rule '^T1Reprocess'" "${command}" "rm" "-i ctaeos -n powerusers --ar ^T1Reprocess" || exit 1
test_assert || exit 1


# Group Mount Rule (gmr)
test_start "group mount rule" "gmr"
test_and_check_cmd "Adding group mount rule 'eosusers2'" "${command}" "add" "-i ctaeos -n eosusers2 -u ctasystest -m 'cta-admin systest add gmr'"\
  'select(.diskInstance=="ctaeos" and .groupMountRule=="eosusers2" and .mountPolicy=="ctasystest" and .comment=="cta-admin systest add gmr") | .comment'\
  "1" "adding group mount request" || exit 1
test_and_check_cmd "Changing group mount rule 'eosusers2' mount policy" "${command}" "ch" "-i ctaeos -n eosusers2 -u repack_ctasystest"\
  'select(.diskInstance=="ctaeos" and .groupMountRule=="eosusers2" and .mountPolicy=="repack_ctasystest") | .comment'\
  "1" "changing group mount rule"|| exit 1
test_command "Removing group mount rule 'eosusers2'" "${command}" "rm" "-i ctaeos -n eosusers2" || exit 1
test_assert || exit 1


# Requester Mount Rule (rmr)
test_start "requester mount rule" "rmr"
test_and_check_cmd "Adding requester mount rule 'adm2'" "${command}" "add" "-i ctaeos -n adm2 -u ctasystest -m 'cta-admin systest add rmr'"\
  'select(.diskInstance=="ctaeos" and .requesterMountRule=="adm2" and .mountPolicy=="ctasystest" and .comment=="cta-admin systest add rmr") | .comment'\
  "1" "adding requester mount rule" || exit 1
test_and_check_cmd "Changing requester mount rule 'adm2'" "${command}" "ch" "-i ctaeos -n adm2 -u repack_ctasystest -m 'cta-admin systest ch rmr'"\
  'select(.diskInstance=="ctaeos" and .requesterMountRule=="adm2" and .mountPolicy=="repack_ctasystest" and .comment=="cta-admin systest ch rmr") | .comment'\
  "1" "changing requester mount rule" || exit 1
test_command "Removing requester mount rule 'adm2'" "${command}" "rm" "-i ctaeos -n adm2" || exit 1
test_assert || exit 1


# Archive Route (ar)
test_start "archive route" "ar"
test_and_check_cmd "Adding archive route" "${command}" "add" "-s ctaStorageClass -c 2 --art DEFAULT -t ctasystest_A -m 'cta-admin systest add'"\
  'select(.storageClass=="ctaStorageClass" and .copyNumber==2 and .tapepool=="ctasystest_A" and .comment=="cta-admin systest add") | .storageClass'\
  "1" "adding archive route" || exit 1
test_and_check_cmd "Changing archive route" "${command}" "ch" "-s ctaStorageClass -c 2 --art DEFAULT -t ctasystest_B -m 'cta-admin systest ch'"\
  'select(.storageClass=="ctaStorageClass" and .copyNumber==2 and .tapepool=="ctasystest_B" and .comment=="cta-admin systest ch") | .storageClass'\
  "1" "changing archive route" || exit 1
test_command "Removing archive route" "${command}" "rm" "-s ctaStorageClass -c 2 --art DEFAULT" || exit 1
test_assert || exit 1


# Mount Policy (mp)
test_start "mount policy" "mp"
test_and_check_cmd "Adding mount policy 'ctasystest2'" "${command}" "add" "-n ctasystest2 --ap 3 --aa 2 --rp 2 --ra 1 -m 'cta-admin systest add'"\
  'select(.name=="ctasystest2" and .archivePriority=="3" and .archiveMinRequestAge=="2" and .retrievePriority=="2" and .retrieveMinRequestAge=="1" and .comment=="cta-admin systest add") | .name'\
  "1" "adding mount policy" || exit 1
test_and_check_cmd "Changing mount policy 'ctasystest2'" "${command}" "ch" "-n ctasystest2 --ap 4 -m 'cta-admin systest ch'"\
  'select(.name=="ctasystest2" and .archivePriority=="4" and .archiveMinRequestAge=="2" and .retrievePriority=="2" and .retrieveMinRequestAge=="1" and .comment=="cta-admin systest ch") | .name'\
  "1" "changing mount policy" || exit 1
test_command "Removing mount policy 'ctasystest2'" "${command}" "rm" "-n ctasystest2" || exit 1
test_assert|| exit 1


# Storage Class (sc)
test_start "storage class" "sc"
test_and_check_cmd "Adding storage class 'ctaStorageClass_4_copy'" "${command}" "add" "-n ctaStorageClass_4_copy -c 1 --vo vo -m 'cta-admin systest add'"\
  'select(.name=="ctaStorageClass_4_copy" and .nbCopies=="1" and .vo=="vo" and .comment=="cta-admin systest add") | .name'\
  "1" "adding storage class" || exit 1
test_and_check_cmd "Change storage class 'ctaStorageClass_4_copy'" "${command}" "ch" "-n ctaStorageClass_4_copy -c 2 -m 'cta-admin systest ch'"\
  'select(.name=="ctaStorageClass_4_copy" and .nbCopies=="2" and .vo=="vo" and .comment=="cta-admin systest ch") | .name'\
  "1" "changing storage class" || exit 1
test_command "Deleting storage class 'ctaStorageClass_4_copy'" "${command}" "rm" "-n ctaStorageClass_4_copy" || exit 1
test_assert || exit 1


########################################
# Requests - sq, re, fr ################
########################################
test_header 'requests'


# Show queue (sq)
# test_start "show queue" "sq"
echo -e "\tTesting cta-admin show queues (sq)"
command=sq
start=$(admin_cta sq)
admin_cta --json ${command} | jq -r 'del(.[] | .creationLog,.lastModificationLog | .time,.host)' >> ${log_file}

# Put drive down and try to archive some files.
for drive in "${dr_names[@]}"; do
  log_message "Putting drive $drive down."
  admin_cta dr down "$drive" -r "cta-admin sq test"
  sleep 3
done
#admin_cta dr down VDSTK11 -r "cta-admin sq test"
xrdcp /root/testfile root://${EOSINSTANCE}//eos/ctaeos/cta/testfile

sleep 10

res=$(admin_cta --json ${command} | jq -r '.[] | select(.mountType=="ARCHIVE_FOR_USER") | .mountType' | wc -l)
#log_command=$(admin_cta --json ${command} | jq -r '.[]')
admin_cta --json ${command} | jq -r 'del(.[] | .creationLog,.lastModificationLog | .time,.host)' >> ${log_file}
if test "${res}" -eq 1; then log_message "Succesfully showed request in queue."; else  { log_message "Error. No request in queue."; exit 1; }; fi

# Put drives back up
for drive in "${dr_names[@]}"; do
  log_message "Putting drive $drive up."
  admin_cta dr up "$drive"
done
#admin_cta dr up VDSTK11

log_message "Waiting for drive to be up."
sleep 5
echo
echo


# Failed requests (fr)
#test_start "failed requests" "fr"
#echo "${vid}" > /root/message.txt

# Wait for file corruption.
#TIMEOUT=30
#SECONDS_PASSED=0
#while [ -z "${RESPONSE}" ]; do
#  log_message "Waiting for tape to be corrupted (${SECONDS_PASSED}s)..."
#  if test ${SECONDS_PASSED} -eq ${TIMEOUT}; then
#    log_message "Error. Timed out while waiting for tape to be corrupted."
#    exit 1
#  fi

#  RESPONSE=$(ls /root/ | grep response.txt)
#  SECONDS_PASSED=$((${SECONDS_PASSED} + 1))
#  sleep 1
#done

#KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOSINSTANCE} prepare -s /eos/ctaeos/cta/testfile

# Wait for fr to fail.
#TIMEOUT=90
#SECONDS_PASSED=0
#while [[ 1 ]]; do
#  log_message "Waiting for request to fail (${SECONDS_PASSED}s)..."
#  if [[ ${SECONDS_PASSED} -eq ${TIMEOUT} ]]; then
#    log_message "Error. Timed out while waiting for request to fail."
#    exit 1
#  fi

#  admin_cta sq
#  admin_cta fr ls
#  res=$(admin_cta --json fr ls | jq -r '.[] | select(.requestType=="RETRIEVE_REQUEST") | .requestType' | wc -l)
#  if [[ ${res} -eq 1 ]]; then
#    break
#  fi
#  SECONDS_PASSED=$((${SECONDS_PASSED} + 1))
#  sleep 1
#done

#log_message "Succesfully tested fail request." || { log_message "Error. Could not generate fail request."; exit 1; }

#fr_id=$(admin_cta --json ${command} ls | jq -r '.[] | .objectId')
#test_command "Remove failed request" ${command} "rm" "-o ${fr_id}" || exit 1

#echo "1" > /root/done.txt

#test_assert  || exit 1


# Repack (re)
test_start "repack" "re"

log_message "Setting tape to full"
admin_cta ta ch -v "${vids[0]}" -f true
sleep 3
log_message "Adding a VO for repacking"
log_message "Setting tape to REPACKING"
admin_cta ta ch -v "${vids[0]}" -s REPACKING -r 'Test repack' || exit 1
sleep 2
admin_cta ta ch -v "${vids[0]}" -s REPACKING -r 'Test repack' || exit 1
sleep 2
TIMEOUT=30
SECS=0
while true; do
  if [[ ${SECS} -eq ${TIMEOUT} ]]; then
    log_message "Timed out while waiting for REPACKING state."
    exit 1
  fi
  log_message "Waiting for tape to change to REPACKING state (${SECS}s)..."
  res=$(admin_cta --json ta ls --all | jq -r ".[] | select(.vid==\"${vids[0]}\") | .state")
  if test "$res" == "REPACKING"; then
    break
  fi
  sleep 1
  SECS=$(( ${SECS} + 1 ))
done

# Repack should fail as system is not correctly configured.
test_command "Adding repack request" "${command}" "add" "-v ${vids[0]} -m -u repack_ctasystest -b root://${EOSINSTANCE}//eos/ctaeos/cta"\
  "select(.vid==\"${vids[0]}\" and .repackBufferUrl=\"root://ctaeos//eos/ctaeos/cta\") | .vid"\
  "1" "adding repack request" || exit 1
test_command "Deleting repack request" "${command}" "rm" "-v ${vids[0]}" || exit 1

test_assert || exit 1


log_message "OK. All tests passed."
exit 0
