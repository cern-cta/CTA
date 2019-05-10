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

REPACK_BUFFER_URL=/eos/ctaeos/repack
vidToRepack1=$(getFirstVidContainingFiles)
if [ "$vidToRepack1" != "null" ] 
then
  echo
  echo "Creating the repack buffer URL directory (${REPACK_BUFFER_URL})"
  kubectl -n ${NAMESPACE} exec ctaeos -- eos mkdir ${REPACK_BUFFER_URL}
  kubectl -n ${NAMESPACE} exec ctaeos -- eos chmod 1777 ${REPACK_BUFFER_URL}

  echo "Marking tape $vidToRepack1 as full before repacking"
  kubectl -n ${NAMESPACE} exec ctacli -- cta-admin ta ch -v $vidToRepack1 -f true

  kubectl -n ${NAMESPACE} cp repack_systemtest.sh client:/root/repack_systemtest.sh

  echo
  echo "Launching the repack test on VID ${vidToRepack1}"
  kubectl -n ${NAMESPACE} exec client -- bash /root/repack_systemtest.sh -v ${vidToRepack1} -b ${REPACK_BUFFER_URL}

  echo "Reclaiming tape $vidToRepack1"
  executeReclaim $vidToRepack1
  echo
  writeTapeSummary $vidToRepack1
else
  echo "No vid found to repack"
  exit 1
fi

vidToRepack2=$(getFirstVidContainingFiles)
if [ "$vidToRepack2" != "null" ] 
then
  echo
  echo "Creating the repack buffer URL directory (${REPACK_BUFFER_URL})"
  kubectl -n ${NAMESPACE} exec ctaeos -- eos mkdir ${REPACK_BUFFER_URL}
  kubectl -n ${NAMESPACE} exec ctaeos -- eos chmod 1777 ${REPACK_BUFFER_URL}

  echo "Marking tape $vidToRepack2 as full before repacking"
  kubectl -n ${NAMESPACE} exec ctacli -- cta-admin ta ch -v $vidToRepack2 -f true
  kubectl -n ${NAMESPACE} cp repack_systemtest.sh client:/root/repack_systemtest.sh

  echo
  echo "Launching the repack test on VID ${vidToRepack2}"
  kubectl -n ${NAMESPACE} exec client -- bash /root/repack_systemtest.sh -v ${vidToRepack2} -b ${REPACK_BUFFER_URL}

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
