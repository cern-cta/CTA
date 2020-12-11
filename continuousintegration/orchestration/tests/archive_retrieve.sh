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
if [ $? -ne 0 ]; then
  echo "ERROR: failed to prepare namespace for the tests"
  exit 1
fi

echo
echo "Launching simple_client_ar.sh on client pod"
echo " Archiving file: xrdcp as user1"
echo " Retrieving it as poweruser1"
kubectl -n ${NAMESPACE} cp simple_client_ar.sh client:/root/simple_client_ar.sh
kubectl -n ${NAMESPACE} cp client_helper.sh client:/root/client_helper.sh
kubectl -n ${NAMESPACE} exec client -- bash /root/simple_client_ar.sh || exit 1

kubectl -n ${NAMESPACE} cp grep_xrdlog_mgm_for_error.sh ctaeos:/root/grep_xrdlog_mgm_for_error.sh
kubectl -n ${NAMESPACE} exec ctaeos -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

NB_FILES=50
FILE_SIZE_KB=15

echo
echo "Launching client_ar.sh on client pod"
echo " Archiving ${NB_FILES} files of ${FILE_SIZE_KB}kB each"
echo " Archiving files: xrdcp as user1"
echo " Retrieving them as poweruser1"
kubectl -n ${NAMESPACE} cp client_ar.sh client:/root/client_ar.sh
kubectl -n ${NAMESPACE} cp client_ar_abortPrepare.py client:/root/client_abortPrepare.sh
kubectl -n ${NAMESPACE} exec client -- bash /root/client_ar.sh -n ${NB_FILES} -s ${FILE_SIZE_KB} -p 100 -d /eos/ctaeos/preprod -v -A || exit 1

kubectl -n ${NAMESPACE} exec ctaeos -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

exit 0
