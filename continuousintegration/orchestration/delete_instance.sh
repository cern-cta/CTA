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

echo "Getting stdout logs of pods"
mkdir -p ../../pod_logs/${instance}
for podcontainer in "init -c ctainit" "ctacli -c ctacli" "ctaeos -c mgm" "ctafrontend -c ctafrontend" "kdc -c kdc" "tpsrv -c taped" "tpsrv -c rmcd"; do
  kubectl --namespace ${instance} logs ${podcontainer} > ../../pod_logs/${instance}/$(echo ${podcontainer} | sed -e 's/ -c /-/').log
done


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
