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

NB_FILES=1
FILE_SIZE_KB=15

kubectl -n ${NAMESPACE} cp client_helper.sh client:/root/client_helper.sh

echo
echo "Launching client_ar.sh on client pod"
echo " Archiving ${NB_FILES} files of ${FILE_SIZE_KB}kB each"
echo " Archiving files: xrdcp as user1"
kubectl -n ${NAMESPACE} cp client_ar.sh client:/root/client_ar.sh
kubectl -n ${NAMESPACE} exec client -- bash /root/client_ar.sh -n ${NB_FILES} -s ${FILE_SIZE_KB} -p 100 -d /eos/ctaeos/preprod -v -A || exit 1

REPACK_BUFFER_URL=/eos/ctaeos/repack
echo "Creating the repack buffer URL directory (${REPACK_BUFFER_URL})"
kubectl -n ${NAMESPACE} exec ctaeos -- eos mkdir ${REPACK_BUFFER_URL}
kubectl -n ${NAMESPACE} exec ctaeos -- eos chmod 1777 ${REPACK_BUFFER_URL}

source ./repack_helper.sh
kubectl -n ${NAMESPACE} cp repack_systemtest.sh client:/root/repack_systemtest.sh

echo
echo "Launching a round trip repack request"

VID_TO_REPACK=$(getFirstVidContainingFiles)
if [ "$VID_TO_REPACK" != "null" ] 
then
echo
  echo "Launching the repack test on VID ${VID_TO_REPACK}"
  kubectl -n ${NAMESPACE} exec client -- bash /root/repack_systemtest.sh -v ${VID_TO_REPACK} -b ${REPACK_BUFFER_URL} || exit 1
else
  echo "No vid found to repack"
  exit 1
fi

echo "Reclaiming tape ${VID_TO_REPACK}"
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin tape reclaim --vid ${VID_TO_REPACK}

VID_TO_REPACK=$(getFirstVidContainingFiles)
if [ "$VID_TO_REPACK" != "null" ] 
then
echo
  echo "Launching the repack test on VID ${VID_TO_REPACK}"
  kubectl -n ${NAMESPACE} exec client -- bash /root/repack_systemtest.sh -v ${VID_TO_REPACK} -b ${REPACK_BUFFER_URL} || exit 1
else
  echo "No vid found to repack"
  exit 1
fi

echo "Reclaiming tape ${VID_TO_REPACK}"
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin tape reclaim --vid ${VID_TO_REPACK}

NB_FILES=1152
kubectl -n ${NAMESPACE} exec client -- bash /root/client_ar.sh -n ${NB_FILES} -s ${FILE_SIZE_KB} -p 100 -d /eos/ctaeos/preprod -v -A || exit 1

VID_TO_REPACK=$(getFirstVidContainingFiles)
if [ "$VID_TO_REPACK" != "null" ] 
then
echo
  echo "Launching the repack test on VID ${VID_TO_REPACK}"
  kubectl -n ${NAMESPACE} exec client -- bash /root/repack_systemtest.sh -v ${VID_TO_REPACK} -b ${REPACK_BUFFER_URL} || exit 1
else
  echo "No vid found to repack"
  exit 1
fi
