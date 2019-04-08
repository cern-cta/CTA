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

executeReclaim() {
    kubectl -n ${NAMESPACE} exec ctacli -ti -- cta-admin ta reclaim -v $1
    echo "Tape $1 reclaimed"
}

executeRepack() {
    WAIT_FOR_REPACK_FILE_TIMEOUT=300
    echo
    echo "Changing the tape $1 to FULL status"
    kubectl -n ${NAMESPACE} exec ctacli -ti -- cta-admin ta ch -v $1 -f true
    echo "Creating the eos directory to put the retrieve files from the repack request"
    kubectl -n ${NAMESPACE} exec ctacli -ti -- rm -rf root://ctaeos.cta.svc.cluster.local:1094//eos/ctaeos/repack
    kubectl -n ${NAMESPACE} exec ctaeos -ti -- eos mkdir /eos/ctaeos/repack
    kubectl -n ${NAMESPACE} exec ctaeos -ti -- eos chmod 1777 /eos/ctaeos/repack
    echo "Removing an eventual previous repack request for tape $1"
    kubectl -n ${NAMESPACE} exec ctacli -ti -- cta-admin re rm -v $1
    echo "Launching the repack request on tape $1"
    kubectl -n ${NAMESPACE} exec ctacli -ti -- cta-admin re add -v $1 -m -b root://ctaeos.cta.svc.cluster.local:1094//eos/ctaeos/repack
    SECONDS_PASSED=0
    while test 0 = `kubectl -n ${NAMESPACE} exec ctacli -ti -- cta-admin re ls -v $1 | grep -E "Complete|Failed" | wc -l`; do
      echo "Waiting for repack request on tape $1 to be complete: Seconds passed = $SECONDS_PASSED"
      sleep 1
      let SECONDS_PASSED=SECONDS_PASSED+1

      if test ${SECONDS_PASSED} == ${WAIT_FOR_REPACK_FILE_TIMEOUT}; then
        echo "Timed out after ${WAIT_FOR_REPACK_FILE_TIMEOUT} seconds waiting for tape $1 to be repacked"
        exit 1
      fi
    done
    if test 1 = `kubectl -n ${NAMESPACE} exec ctacli -ti -- cta-admin re ls -v $1 | grep -E "Failed" | wc -l`; then
        echo "Repack failed for tape $1"
        exit 1
    fi
}

echo "Execution of simple_repack.sh"

kubectl -n ${NAMESPACE} exec ctacli -ti -- cta-admin ta ls --all -h

executeRepack V01001

sleep 1

echo "Reclaiming tape V01001"
executeReclaim V01001

executeRepack V01003

echo
echo "Summary of the content of the tapes"
kubectl -n ${NAMESPACE} exec ctacli -ti -- cta-admin ta ls -v V01001 -h
kubectl -n ${NAMESPACE} exec ctacli -ti -- cta-admin ta ls -v V01003 -h

echo
echo "Summary of the repack requests"
kubectl -n ${NAMESPACE} exec ctacli -ti -- cta-admin re ls -h

echo "Reclaiming tape V01003"
executeReclaim V01003

echo "End of test simple_repack"