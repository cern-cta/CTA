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

source ./repack_helper.sh

echo "Execution of simple_repack.sh"

vidToRepack1=$(getVidToRepack)
if [ "$vidToRepack1" != "null" ] 
then
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

vidToRepack2=$(getVidToRepack)
if [ "$vidToRepack2" != "null" ] 
then
  echo
  writeTapeSummary $vidToRepack2
  executeRepack $vidToRepack2
  echo
  echo "Reclaiming tape $vidToRepack2"
  executeReclaim $vidToRepack2
  echo
  writeTapeSummary $vidToRepack1
  writeTapeSummary $vidToRepack2
else
  echo "No vid found to repack"
  exit 1
fi

echo
echo
echo "Summary of the repack requests"
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin re ls -h

echo "End of test simple_repack"
