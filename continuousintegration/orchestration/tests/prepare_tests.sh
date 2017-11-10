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


# eos instance identified by SSS username
EOSINSTANCE=ctaeos

tempdir=$(mktemp -d) # temporary directory for system test related config
echo -n "Reading library configuration from tpsrv01"
SECONDS_PASSED=0
while test 0 = $(kubectl --namespace ${NAMESPACE} exec tpsrv01 -c taped -- cat /tmp/library-rc.sh | sed -e 's/^export//' | tee ${tempdir}/library-rc.sh | wc -l); do
  sleep 1
  echo -n .
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == 30; then
    echo "FAILED"
    echo "Timed out after ${SECONDS_PASSED} seconds waiting for tape library configuration."
    exit 1
  fi
done
echo "OK"

echo "Using this configuration for library:"
cat ${tempdir}/library-rc.sh
. ${tempdir}/library-rc.sh

#clean the  library
#  echo "Clean the library /dev/${LIBRARYDEVICE} if needed"
#    mtx -f /dev/${LIBRARYDEVICE} status | sed -e "s/:/ /g"| grep "Full" | awk '{if ($1=="Data" ) { rewind="mt -f /dev/${DRIVEDEVICES["$4"]} rewind"; print rewind; print "Rewind drive "$4>"/dev/stderr"; unload="mtx -f /dev/${LIBRARYDEVICE} unload "$8" "$4; print unload; print "Unloading to storage slot "$8" from data slot "$4"" >"/dev/stderr";}}' |  source /dev/stdin

ctacliIP=`kubectl --namespace ${NAMESPACE} describe pod ctacli | grep IP | sed -E 's/IP:[[:space:]]+//'`

echo "Preparing CTA configuration for tests"
  kubectl --namespace ${NAMESPACE} exec ctafrontend -- cta-catalogue-admin-host-create /etc/cta/cta-catalogue.conf --hostname ${ctacliIP} -c "docker cli"
  kubectl --namespace ${NAMESPACE} exec ctafrontend -- cta-catalogue-admin-user-create /etc/cta/cta-catalogue.conf --username ctaadmin1 -c "docker cli"
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta logicallibrary add \
     --name ${LIBRARYNAME}                                            \
     --comment "ctasystest"                                           
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta tapepool add       \
    --name ctasystest                                                 \
    --partialtapesnumber 5                                            \
    --encrypted false                                                 \
    --comment "ctasystest"                                            
  # add all tapes
  for ((i=0; i<${#TAPES[@]}; i++)); do
    VID=${TAPES[${i}]}
    kubectl --namespace ${NAMESPACE} exec ctacli -- cta tape add         \
      --logicallibrary ${LIBRARYNAME}                                 \
      --tapepool ctasystest                                           \
      --capacity 1000000000                                           \
      --comment "ctasystest"                                          \
      --vid ${VID}                                                    \
      --disabled false                                                \
      --full false                                                    \
      --comment "ctasystest"
  done
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta storageclass add   \
    --instance ${EOSINSTANCE}                                            \
    --name ctaStorageClass                                            \
    --copynb 1                                                        \
    --comment "ctasystest"
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta archiveroute add   \
    --instance ${EOSINSTANCE}                                         \
    --storageclass ctaStorageClass                                    \
    --copynb 1                                                        \
    --tapepool ctasystest                                             \
    --comment "ctasystest"
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta mountpolicy add    \
    --name ctasystest                                                 \
    --archivepriority 1                                               \
    --minarchiverequestage 1                                          \
    --retrievepriority 1                                              \
    --minretrieverequestage 1                                         \
    --maxdrivesallowed 1                                              \
    --comment "ctasystest"
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta requestermountrule add \
     --instance ${EOSINSTANCE}                                        \
     --name adm                                                       \
     --mountpolicy ctasystest --comment "ctasystest"
###
# This rule exists to allow users from eosusers group to migrate files to tapes
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta groupmountrule add \
     --instance ${EOSINSTANCE}                                        \
     --name eosusers                                                  \
     --mountpolicy ctasystest --comment "ctasystest"
###
# This rule exists to allow users from powerusers group to recall files from tapes
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta groupmountrule add \
     --instance ${EOSINSTANCE}                                        \
     --name powerusers                                                  \
     --mountpolicy ctasystest --comment "ctasystest"

echo "Setting drive up: ${DRIVENAMES[${driveslot}]}"
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta drive up ${DRIVENAMES[${driveslot}]}
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta drive ls

# A bit of reporting
echo "EOS server version is used:"
  kubectl --namespace ${NAMESPACE} exec ctaeos -- rpm -qa|grep eos-server


# Super client capabilities
echo "Adding super client capabilities"

clientIP=`kubectl --namespace ${NAMESPACE} describe pod client | grep IP | sed -E 's/IP:[[:space:]]+//'`
kubectl --namespace ${NAMESPACE} exec ctacli -- cta adminhost add --name ${clientIP} --comment "for the super client"
kubectl --namespace ${NAMESPACE} exec ctacli -- cta admin add --username ctaadmin2 --comment "ctaadmin2"

kubectl --namespace=${NAMESPACE} exec kdc cat /root/ctaadmin2.keytab | kubectl --namespace=${NAMESPACE} exec -i client --  bash -c "cat > /root/ctaadmin2.keytab; mkdir -p /tmp/ctaadmin2"
