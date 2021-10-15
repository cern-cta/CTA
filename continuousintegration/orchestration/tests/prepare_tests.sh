#!/bin/bash

# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2021 CERN
# @license        This program is free software: you can redistribute it and/or modify
#                 it under the terms of the GNU General Public License as published by
#                 the Free Software Foundation, either version 3 of the License, or
#                 (at your option) any later version.
#
#                 This program is distributed in the hope that it will be useful,
#                 but WITHOUT ANY WARRANTY; without even the implied warranty of
#                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#                 GNU General Public License for more details.
#
#                 You should have received a copy of the GNU General Public License
#                 along with this program.  If not, see <http://www.gnu.org/licenses/>.

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

# Get list of tape drives that have a tape server
TAPEDRIVES_IN_USE=()
for tapeserver in $(kubectl --namespace ${NAMESPACE} get pods | grep tpsrv | awk '{print $1}'); do
  TAPEDRIVES_IN_USE+=($(kubectl --namespace ${NAMESPACE} exec ${tapeserver} -c taped -- cat /etc/cta/TPCONFIG | awk '{print $1}'))
done
NB_TAPEDRIVES_IN_USE=${#TAPEDRIVES_IN_USE[@]}

echo "Preparing CTA configuration for tests"
  # verify the catalogue DB schema
    kubectl --namespace ${NAMESPACE} exec ctafrontend -- cta-catalogue-schema-verify /etc/cta/cta-catalogue.conf
    if [ $? -ne 0 ]; then
      echo "ERROR: failed to verify the catalogue DB schema"
      exit 1
    fi
  kubectl --namespace ${NAMESPACE} exec ctafrontend -- cta-catalogue-admin-user-create /etc/cta/cta-catalogue.conf --username ctaadmin1 -m "docker cli"

echo 'kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin --json version | jq'
kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin --json version | jq

  echo "Cleaning up leftovers from potential previous runs."
  kubectl --namespace ${NAMESPACE} exec ctaeos -- eos rm /eos/ctaeos/cta/*
  kubectl --namespace ${NAMESPACE} exec ctaeos -- eos find -f /eos/ctaeos/preprod/ | xargs -I{} kubectl --namespace ${NAMESPACE} exec ctaeos -- eos rm -rf {}
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin --json tape ls --all  |             \
    jq -r '.[] | .vid ' | xargs -I{} kubectl --namespace ${NAMESPACE} exec ctacli --            \
    cta-admin tape rm -v {}

  kubectl --namespace ${NAMESPACE}  exec ctacli -- cta-admin --json archiveroute ls |           \
    jq '.[] |  " -s " + .storageClass + " -c " + (.copyNumber|tostring)' | \
    xargs -I{} bash -c "kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin archiveroute rm {}"

  kubectl --namespace ${NAMESPACE}  exec ctacli -- cta-admin --json tapepool ls  |              \
    jq -r '.[] | .name' |                                                                       \
    xargs -I{} kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin tapepool rm -n {}

  kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin --json storageclass ls  |           \
    jq -r '.[] | " -n  " + .name'  |                                    \
    xargs -I{} bash -c "kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin storageclass rm {}"

  kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin --json vo ls  |           \
    jq -r '.[] | " --vo  " + .name'  |                                    \
    xargs -I{} bash -c "kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin vo rm {}"


  for ((i=0; i<${#TAPEDRIVES_IN_USE[@]}; i++)); do
    kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin logicallibrary add \
      --name ${TAPEDRIVES_IN_USE[${i}]}                                            \
      --comment "ctasystest library mapped to drive ${TAPEDRIVES_IN_USE[${i}]}"
  done

  kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin virtualorganization add  \
    --vo vo                                                                          \
    --readmaxdrives 1                                                                \
    --writemaxdrives 1                                                               \
    --comment "vo"
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin tapepool add       \
    --name ctasystest                                                 \
    --vo vo                                                           \
    --partialtapesnumber 5                                            \
    --encrypted false                                                 \
    --comment "ctasystest"
  # add the media types of the tapes in production
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin mediatype add \
    --name T10K500G  \
    --capacity 500000000000 \
    --primarydensitycode 74 \
    --cartridge "T10000" \
    --comment "Oracle T10000 cartridge formated at 500 GB (for developers only)"
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin mediatype add \
    --name 3592JC7T \
    --capacity 7000000000000 \
    --primarydensitycode 84 \
    --cartridge "3592JC" \
    --comment "IBM 3592JC cartridge formated at 7 TB"
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin mediatype add \
    --name 3592JD15T \
    --capacity 15000000000000 \
    --primarydensitycode 85 \
    --cartridge "3592JD" \
    --comment "IBM 3592JD cartridge formated at 15 TB"
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin mediatype add \
    --name 3592JE20T \
    --capacity 20000000000000 \
    --primarydensitycode 87 \
    --cartridge "3592JE" \
    --comment "IBM 3592JE cartridge formated at 20 TB"
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin mediatype add \
    --name LTO7M \
    --capacity 9000000000000 \
    --primarydensitycode 93 \
    --cartridge "LTO-7" \
    --comment "LTO-7 M8 cartridge formated at 9 TB"
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin mediatype add \
    --name LTO8 \
    --capacity 12000000000000 \
    --primarydensitycode 94 \
    --cartridge "LTO-8" \
    --comment "LTO-8 cartridge formated at 12 TB"
  # add all tapes
  for ((i=0; i<${#TAPES[@]}; i++)); do
    VID=${TAPES[${i}]}
    kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin tape add     \
      --mediatype "T10K500G"                                              \
      --vendor vendor                                                      \
      --logicallibrary ${TAPEDRIVES_IN_USE[${i}%${NB_TAPEDRIVES_IN_USE}]}  \
      --tapepool ctasystest                                                \
      --comment "ctasystest"                                               \
      --vid ${VID}                                                         \
      --full false                                                         \
      --comment "ctasystest"
  done
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin storageclass add   \
    --name ctaStorageClass                                            \
    --numberofcopies 1                                                        \
    --vo vo                                                           \
    --comment "ctasystest"
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin archiveroute add   \
    --storageclass ctaStorageClass                                    \
    --copynb 1                                                        \
    --tapepool ctasystest                                             \
    --comment "ctasystest"
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin mountpolicy add    \
    --name ctasystest                                                 \
    --archivepriority 1                                               \
    --minarchiverequestage 1                                          \
    --retrievepriority 1                                              \
    --minretrieverequestage 1                                         \
    --comment "ctasystest"
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin requestermountrule add \
     --instance ${EOSINSTANCE}                                        \
     --name adm                                                       \
     --mountpolicy ctasystest --comment "ctasystest"

###
# This rule exists to allow users from eosusers group to migrate files to tapes
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin groupmountrule add \
     --instance ${EOSINSTANCE}                                        \
     --name eosusers                                                  \
     --mountpolicy ctasystest --comment "ctasystest"
###
# This rule exists to allow users from powerusers group to recall files from tapes
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin groupmountrule add \
     --instance ${EOSINSTANCE}                                        \
     --name powerusers                                                  \
     --mountpolicy ctasystest --comment "ctasystest"
###
# This mount policy is for repack: IT MUST CONTAIN THE `repack` STRING IN IT TO ALLOW MOUNTING DISABLED TAPES
kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin mountpolicy add    \
    --name repack_ctasystest                                                 \
    --archivepriority 2                                               \
    --minarchiverequestage 1                                          \
    --retrievepriority 2                                              \
    --minretrieverequestage 1                                         \
    --comment "repack mountpolicy for ctasystest"

###
# This rule if for retrieves, and matches the retrieve activity used in the tests only
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin activitymountrule add \
     --instance ${EOSINSTANCE}                                        \
     --name powerusers                                                \
     --activityregex ^T0Reprocess$                                    \
     --mountpolicy ctasystest --comment "ctasystest"

echo "Labeling tapes:"
  # add all tapes
  for ((i=0; i<${#TAPES[@]}; i++)); do
    VID=${TAPES[${i}]}
    echo "  cta-tape-label --vid ${VID}"
    # for debug use
      # kubectl --namespace ${NAMESPACE} exec tpsrv01 -c taped  -- cta-tape-label --vid ${VID} --debug
    kubectl --namespace ${NAMESPACE} exec tpsrv01 -c taped  -- cta-tape-label --vid ${VID}
    if [ $? -ne 0 ]; then
      echo "ERROR: failed to label the tape ${VID}"
      exit 1
    fi
  done

echo "Setting drive up: ${DRIVENAMES[${driveslot}]}"
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin drive up ${DRIVENAMES[${driveslot}]}
  kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin drive ls

# A bit of reporting
echo "EOS server version is used:"
  kubectl --namespace ${NAMESPACE} exec ctaeos -- rpm -qa|grep eos-server


# Super client capabilities
echo "Adding super client capabilities"

clientIP=`kubectl --namespace ${NAMESPACE} describe pod client | grep IP | sed -E 's/IP:[[:space:]]+//'`
kubectl --namespace ${NAMESPACE} exec ctacli -- cta-admin admin add --username ctaadmin2 --comment "ctaadmin2"

kubectl --namespace=${NAMESPACE} exec kdc cat /root/ctaadmin2.keytab | kubectl --namespace=${NAMESPACE} exec -i client --  bash -c "cat > /root/ctaadmin2.keytab; mkdir -p /tmp/ctaadmin2"
kubectl --namespace=${NAMESPACE} exec kdc cat /root/poweruser1.keytab | kubectl --namespace=${NAMESPACE} exec -i client --  bash -c "cat > /root/poweruser1.keytab; mkdir -p /tmp/poweruser1"
kubectl --namespace=${NAMESPACE} exec kdc cat /root/eosadmin1.keytab | kubectl --namespace=${NAMESPACE} exec -i client --  bash -c "cat > /root/eosadmin1.keytab; mkdir -p /tmp/eosadmin1"

###
# Filling services in DNS on all pods
###
# Generate hosts file for all defined services
TMP_HOSTS=$(mktemp)
KUBERNETES_DOMAIN_NAME='svc.cluster.local'
KUBEDNS_IP=$(kubectl -n kube-system get service kube-dns -o json | jq -r '.spec.clusterIP')
for service in $(kubectl --namespace=${NAMESPACE} get service -o json | jq -r '.items[].metadata.name'); do
  service_IP=$(nslookup -timeout=1 ${service}.${NAMESPACE}.${KUBERNETES_DOMAIN_NAME} ${KUBEDNS_IP} | grep -A1 ${service}.${NAMESPACE} | grep Address | awk '{print $2}')
  echo "${service_IP} ${service}.${NAMESPACE}.${KUBERNETES_DOMAIN_NAME} ${service}"
done > ${TMP_HOSTS}

# push to all Running containers removing already generated entries
kubectl -n ${NAMESPACE} get pods -o json | jq -r '.items[] | select(.status.phase=="Running") | {name: .metadata.name, containers: .spec.containers[].name} | {command: (.name + " -c " + .containers)}|to_entries[]|(.value)' | while read container; do
  cat ${TMP_HOSTS} | grep -v $(echo ${container} | awk '{print $1}')| kubectl -n ${NAMESPACE} exec ${container} -i -- bash -c "cat >> /etc/hosts"
done
