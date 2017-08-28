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

RC=0

echo
echo "Launching nightly1.sh on client pod"
kubectl -n ${NAMESPACE} cp client_ar.sh client:/root/client_ar.sh
kubectl -n ${NAMESPACE} cp client_helper.sh client:/root/client_helper.sh
kubectl -n ${NAMESPACE} cp nightly1.sh client:/root/nightly1.sh
kubectl -n ${NAMESPACE} exec client -- bash /root/nightly1.sh || ((RC++))
kubectl -n ${NAMESPACE} exec client -- bash /root/nightly1.sh || ((RC++))

echo
echo "EOS workflow summary:"
kubectl -n ${NAMESPACE} exec ctaeos eos -- ls /eos/ctaeos/proc/workflow/$(date +%Y%m%d)/f/default | cut -d: -f3 | sort | uniq -c

exit ${RC}
