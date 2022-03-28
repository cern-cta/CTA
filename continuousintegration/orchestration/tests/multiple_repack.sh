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

#default CI EOS instance
EOSINSTANCE=ctaeos
EOSBASEDIR=/eos/ctaeos/preprod

#default Repack timeout
WAIT_FOR_REPACK_TIMEOUT=300

die() {
  echo "$@" 1>&2
  test -z $TAILPID || kill ${TAILPID} &> /dev/null
  exit 1
}

usage() { cat <<EOF 1>&2
Usage: $0 -s <size_of_tapes> -n <nb_files_per_tape> -b <repack_buffer_url> [-e <eosinstance>] [-d <eosbasedir>] [-t <timeout>]
size_of_tape : in MB
repack_buffer_url example : /eos/ctaeos/repack
eosinstance : the name of the ctaeos instance to be used (default ctaeos)
eosbasedir : the path in which files will be created for archival
timeout : the timeout in seconds to wait for each repack request to be complete
EOF
exit 1
}

if [ $# -lt 6 ]
then
  usage
fi;

while getopts "s:n:e:t:b:d:" o; do
  case "${o}" in
    s)
      SIZE_OF_TAPES=${OPTARG}
      ;;
    n)
      NB_FILES_PER_TAPE=${OPTARG}
      ;;
    e)
      EOSINSTANCE=${OPTARG}
      ;;
    t)
      WAIT_FOR_REPACK_TIMEOUT=${OPTARG}
      ;;
    b)
      REPACK_BUFFER_URL=${OPTARG}
      ;;
    d)
      EOSBASEDIR=${OPTARG}
      ;;
    *)
      usage
      ;;
  esac
done
shift $((OPTIND -1))

if [ -z "${SIZE_OF_TAPES}" ]; then
    usage
fi

if [ -z "${NB_FILES_PER_TAPE}" ]; then
    usage
fi

if [ -z "${REPACK_BUFFER_URL}" ]; then
    usage
fi

echo "Starting multiple repack test"

. /root/client_helper.sh

# Get kerberos credentials for user1
admin_kinit
admin_klist > /dev/null 2>&1 || die "Cannot get kerberos credentials for user ${USER}"

# Get the number of tapes available
# availableTapes=`admin_cta --json ta ls --all | jq -r ".[] | select (.occupancy==\"0\") | .vid"`
availableTapes=`admin_cta --json ta ls --all | jq -r ".[] | select (.full==false) | .vid"`

# Save the available tapes in an array
read -a arrayTapes <<< $availableTapes
nbTapes=${#arrayTapes[@]}

#Get the tapes that we will repack
nbTapesToRepack=$(($nbTapes/2))
tapesToRepack=()
for ((i=0; i<nbTapesToRepack; i++))
do
  tapesToRepack+=(${arrayTapes[$i]})
done

destinationTapes=()
for (( i=$(($nbTapesToRepack)); i<nbTapes; i++ ))
do
  destinationTapes+=(${arrayTapes[$i]})
done

# Mark repack destination tape as full, we don't want to archive on them
for vid in ${destinationTapes[@]}
do
  echo "Marking repack destination tape (${vid}) as full"
  admin_cta tape ch --vid $vid --full true
done

nbDestinationTape=${#destinationTapes[@]}

# Compute the number of files to copy and the size of each file
fileSizeToCopy=`perl -e "use POSIX; print int( ceil((( (${SIZE_OF_TAPES} * 1000) - ((6 * 80) / 1000)) / ${NB_FILES_PER_TAPE})) )"`
nbFilesToCopy=$(($NB_FILES_PER_TAPE * $nbTapesToRepack))

echo
echo "file size to copy (in KB) =  $fileSizeToCopy"
echo "Nb files to copy = $nbFilesToCopy"

bash /root/client_ar.sh -n ${nbFilesToCopy} -s ${fileSizeToCopy} -p 100 -d ${EOSBASEDIR} -v -A || exit 1

for vid in ${destinationTapes[@]}
do
  echo "Marking destination tape (${vid}) as not full"
  admin_cta tape ch --vid $vid --full false
done

allPid=()
for vid in ${tapesToRepack[@]}
do
  echo "Launching repack requests on vid $vid"
  bash /root/repack_systemtest.sh -v $vid -b ${REPACK_BUFFER_URL} -t 500 -n ctasystest &
  allPid+=($!)
done

oneRepackFailed=0
for pid in ${allPid[@]}
do
  wait $pid || oneRepackFailed=1
done

if [[ $oneRepackFailed == 1 ]]
then
  die "Fail of multiple_repack test"
fi

echo "End of multiple_repack test"

#WAIT_FOR_REPACK_TIMEOUT=300
#
#while test $nbTapesToRepack != `admin_cta --json re ls | jq "[.[] | select(.status == \"Complete\" or .status == \"Failed\")] | length"`; do
#  echo "Waiting for repack request on all tapes to be complete: Seconds passed = $SECONDS_PASSED"
#  sleep 1
#  let SECONDS_PASSED=SECONDS_PASSED+1
#
#  if test ${SECONDS_PASSED} == ${WAIT_FOR_REPACK_TIMEOUT}; then
#    echo "Timed out after ${WAIT_FOR_REPACK_TIMEOUT} seconds waiting all tapes to be repacked"
#    exit 1
#  fi
#done
#
#successfulRepackTapes=`admin_cta --json re ls | jq ".[] | select(.status == \"Complete\") | .vid"`
#failedToRepackTapes=`admin_cta --json re ls | jq ".[] | select(.status == \"Failed\") | .vid"`
#
#read -a arrayFailedToRepackTapes <<< $failedToRepackTapes
#
#if  test 0 != ${#arrayFailedToRepackTapes[@]} then
#    echo "Repack failed for tapes ${arrayFailedToRepackTapes[@]}."
#    exit 1
#else
#  for vid in $successfulRepackTapes[@]
#  do
#    bash /root/repack_generate_report.sh -v $vid
#  done
#  echo "End of multiple repack test"
#  exit 0
#fi

# echo $nb_tapes_to_fill
