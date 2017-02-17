#!/bin/bash

usage() { cat <<EOF 1>&2
Usage: $0 -n <namespace>
EOF
exit 1
}

while getopts "n:" o; do
    case "${o}" in
        n)
            instance=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${instance}" ]; then
    usage
fi

if [ ! -z "${error}" ]; then
    echo -e "ERROR:\n${error}"
    exit 1
fi

# Collect the logs
###

# First in a temporary directory so that we can get the logs on the gitlab runner if something bad happens
# indeed if the system test fails, artifacts are not collected for the build
tmpdir=$(mktemp -d -t ${instance}_delete_XXXX)
echo "Collecting stdout logs of pods to ${tmpdir}"
for podcontainer in "init -c ctainit" "ctacli -c ctacli" "ctaeos -c mgm" "ctafrontend -c ctafrontend" "kdc -c kdc" "tpsrv -c taped" "tpsrv -c rmcd"; do
  kubectl --namespace ${instance} logs ${podcontainer} > ${tmpdir}/$(echo ${podcontainer} | sed -e 's/ -c /-/').log
done

if [ -z "${CI_PIPELINE_ID}" ]; then
	# we are in the context of a CI run => save artifacts in the directory structure of the build
	mkdir -p ../../pod_logs/${instance}
	cp -r ${tmpdir}/* ../../pod_logs/${instance}
fi

echo "Deleting ${instance} instance"

kubectl delete namespace ${instance}
for ((i=0; i<120; i++)); do
  echo -n "."
  kubectl get namespace | grep -q "^${instance} " || break
  sleep 1
done

echo OK

./recycle_librarydevice_PV.sh

echo "Status of library pool after test:"
kubectl get pv
