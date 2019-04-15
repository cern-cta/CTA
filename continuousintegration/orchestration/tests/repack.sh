#!/bin/bash

usage() { cat <<EOF 1>&2
Usage: $0 -n <namespace>
EOF
exit 1
}

while getopts "n:" o; do
    case "${o}" in
        n)
            NAMESPACE=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${NAMESPACE}" ]; then
    usage
fi

if [ ! -z "${error}" ]; then
    echo -e "ERROR:\n${error}"
    exit 1
fi

echo "Preparing namespace for the tests"
./prepare_tests.sh -n ${NAMESPACE}

kubectl -n ${NAMESPACE} cp client_helper.sh client:/root/client_helper.sh

NB_FILES=1000
FILE_SIZE_KB=15

echo
echo "Launching client_ar.sh on client pod"
echo " Archiving ${NB_FILES} files of ${FILE_SIZE_KB}kB each"
echo " Archiving files: xrdcp as user1"
kubectl -n ${NAMESPACE} cp client_ar.sh client:/root/client_ar.sh
kubectl -n ${NAMESPACE} exec client -- bash /root/client_ar.sh -n ${NB_FILES} -s ${FILE_SIZE_KB} -p 100 -d /eos/ctaeos/preprod -v -A || exit 1

#kubectl -n ${NAMESPACE} exec ctaeos -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

source ./repack_helper.sh

vidToRepack1=$(getFirstVidContainingFiles)
if [ "$vidToRepack1" != "null" ] 
then
  echo
  echo "Launching a repack on tape $vidToRepack1" 
  echo
  writeTapeSummary $vidToRepack1
  executeRepack $vidToRepack1
  echo
  echo "Reclaiming tape $vidToRepack1"
  executeReclaim $vidToRepack1
  echo
  writeTapeSummary $vidToRepack1
else
  echo "No vid found to repack"
  exit 1
fi

vidDestination=$(getFirstVidContainingFiles)
writeTapeSummary $vidDestination