#!/bin/bash

LOG_DIR=/tmp  # by default writte tar files of logs to /tmp
collectlogs=1   # by default collect all container logs

usage() { cat <<EOF 1>&2
Usage: $0 -n <namespace> [-d <log_directory>] [-D]

Options:
  -D    Discard logs (by default logs are collected before deleting the instance)
EOF
exit 1
}

while getopts "n:d:D" o; do
    case "${o}" in
        n)
            instance=${OPTARG}
            ;;
        d)
            LOG_DIR=${OPTARG}
            ;;
        D)
            collectlogs=0
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

if [ ! -d ${LOG_DIR} ]; then
    echo -e "ERROR: directory ${LOG_DIR} does not exist"
    exit 1
fi
if [ ! -w ${LOG_DIR} ]; then
    echo -e "ERROR: cannot write to directory ${LOG_DIR}"
    exit 1
fi


###
# Display all backtraces if any
###

for backtracefile in $(kubectl --namespace ${instance} exec ctacli -- bash -c 'find /mnt/logs | grep core | grep bt$'); do
  pod=$(echo ${backtracefile} | cut -d/ -f4)
  echo "Found backtrace in pod ${pod}:"
  kubectl --namespace ${instance} exec ctacli -- cat ${backtracefile}
done


###
# Collect the logs?
###
if [ $collectlogs == 1 ] ; then
  # First in a temporary directory so that we can get the logs on the gitlab runner if something bad happens
  # indeed if the system test fails, artifacts are not collected for the build
  tmpdir=$(mktemp --tmpdir=${LOG_DIR} -d -t ${instance}_delete_XXXX)
  echo "Collecting stdout logs of pods to ${tmpdir}"
  for podcontainer in "init -c ctainit" "client -c client" "ctacli -c ctacli" "ctaeos -c mgm" "ctafrontend -c ctafrontend" "kdc -c kdc" "tpsrv01 -c taped" "tpsrv01 -c rmcd" "tpsrv02 -c taped" "tpsrv02 -c rmcd"; do
    kubectl --namespace ${instance} logs ${podcontainer} > ${tmpdir}/$(echo ${podcontainer} | sed -e 's/ -c /-/').log
  done
  kubectl --namespace ${instance} exec ctacli -- tar -C /mnt/logs -zcf - . > ${tmpdir}/varlog.tgz

  if [ ! -z "${CI_PIPELINE_ID}" ]; then
    # we are in the context of a CI run => save artifacts in the directory structure of the build
    echo "Saving logs as artifacts"
    mkdir -p ../../pod_logs/${instance}
    cp -r ${tmpdir}/* ../../pod_logs/${instance}
  fi
else
  # Do not Collect logs
  echo "Discarding logs for the current run"
fi


echo "Deleting ${instance} instance"

kubectl delete namespace ${instance}
for ((i=0; i<120; i++)); do
  echo -n "."
  kubectl get namespace | grep -q "^${instance} " || break
  sleep 1
done

echo OK

# this now useless as dummy NFS PV were replaced by local volumes
# those are recycled automatically
./recycle_librarydevice_PV.sh

echo "Status of library pool after test:"
kubectl get pv
