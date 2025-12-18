#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

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

if [[ -z "${NAMESPACE}" ]]; then
    usage
fi

CLIENT_POD="cta-client-0"
CTA_CLI_POD="cta-cli-0"
CTA_FRONTEND_POD="cta-frontend-0"

DCACHE_INSTANCE_NAME="dcache"

CTA_TPSRV_POD="cta-tpsrv01-0"

# Set the TAPES and DRIVE_NAME based on the config in CTA_TPSRV_POD
echo "Reading library configuration from ${CTA_TPSRV_POD}"
DRIVE_NAME=$(kubectl exec -n ${NAMESPACE} ${CTA_TPSRV_POD} -c cta-taped-0 -- printenv DRIVE_NAME)
LIBRARY_DEVICE=$(kubectl exec -n ${NAMESPACE} ${CTA_TPSRV_POD} -c cta-taped-0 -- printenv LIBRARY_DEVICE)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mapfile -t TAPES < <(${SCRIPT_DIR}/../../utils/tape/list_all_tapes_in_library.sh -d $LIBRARY_DEVICE)
echo "Using drive: $DRIVE_NAME"
echo "Using tapes:"
for VID in "${TAPES[@]}"; do
  echo "- $VID"
done

# Get list of tape drives that have a tape server
TAPEDRIVES_IN_USE=()
for tapeserver in $(kubectl --namespace ${NAMESPACE} get pods | grep tpsrv | awk '{print $1}'); do
  TAPEDRIVES_IN_USE+=($(kubectl --namespace ${NAMESPACE} exec ${tapeserver} -c cta-taped-0 -- bash -c "find /etc/cta | grep cta-taped- | xargs cat" | grep LogicalLibrary | awk 'NR==1 {print $3}'))
done
NB_TAPEDRIVES_IN_USE=${#TAPEDRIVES_IN_USE[@]}

echo "Preparing CTA configuration for tests"
# verify the catalogue DB schema
if ! kubectl --namespace ${NAMESPACE} exec ${CTA_FRONTEND_POD} -c cta-frontend -- cta-catalogue-schema-verify /etc/cta/cta-catalogue.conf; then
  echo "ERROR: failed to verify the catalogue DB schema"
  exit 1
fi
kubectl --namespace ${NAMESPACE} exec ${CTA_FRONTEND_POD} -c cta-frontend -- cta-catalogue-admin-user-create /etc/cta/cta-catalogue.conf --username ctaadmin1 -m "docker cli"

echo 'kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json version | jq'
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json version | jq

for ((i=0; i<${#TAPEDRIVES_IN_USE[@]}; i++)); do
  kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin logicallibrary add \
    --name ${TAPEDRIVES_IN_USE[${i}]}                                            \
    --comment "ctasystest library mapped to drive ${TAPEDRIVES_IN_USE[${i}]}"
done

kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin diskinstance add  \
  --name ${DCACHE_INSTANCE_NAME}                                                    \
  --comment "di"

kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin virtualorganization add  \
  --vo vo                                                                          \
  --readmaxdrives 1                                                                \
  --writemaxdrives 1                                                               \
  --diskinstance ${DCACHE_INSTANCE_NAME}                                                    \
  --comment "vo"

kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin virtualorganization add  \
  --vo vo_repack                                                                   \
  --readmaxdrives 1                                                                \
  --writemaxdrives 1                                                               \
  --diskinstance ${DCACHE_INSTANCE_NAME}                                                    \
  --comment "vo_repack"                                                            \
  --isrepackvo true

# add the media types of the tapes in production
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin mediatype add \
  --name T10K500G  \
  --capacity 500000000000 \
  --primarydensitycode 74 \
  --cartridge "T10000" \
  --comment "Oracle T10000 cartridge formated at 500 GB (for developers only)"
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin mediatype add \
  --name 3592JC7T \
  --capacity 7000000000000 \
  --primarydensitycode 84 \
  --cartridge "3592JC" \
  --comment "IBM 3592JC cartridge formated at 7 TB"
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin mediatype add \
  --name 3592JD15T \
  --capacity 15000000000000 \
  --primarydensitycode 85 \
  --cartridge "3592JD" \
  --comment "IBM 3592JD cartridge formated at 15 TB"
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin mediatype add \
  --name 3592JE20T \
  --capacity 20000000000000 \
  --primarydensitycode 87 \
  --cartridge "3592JE" \
  --comment "IBM 3592JE cartridge formated at 20 TB"
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin mediatype add \
  --name LTO7M \
  --capacity 9000000000000 \
  --primarydensitycode 93 \
  --cartridge "LTO-7" \
  --comment "LTO-7 M8 cartridge formated at 9 TB"
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin mediatype add \
  --name LTO8 \
  --capacity 12000000000000 \
  --primarydensitycode 94 \
  --cartridge "LTO-8" \
  --comment "LTO-8 cartridge formated at 12 TB"

# Setup default tapepool and storage class
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tapepool add \
  --name ctasystest                                                    \
  --vo vo                                                              \
  --partialtapesnumber 5                                               \
  --comment "ctasystest"

kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin storageclass add \
  --name test:tape@cta                                                   \
  --numberofcopies 1                                                       \
  --vo vo                                                                  \
  --comment "ctasystest"
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin archiveroute add \
  --storageclass test:tape@cta                                           \
  --copynb 1                                                               \
  --tapepool ctasystest                                                    \
  --comment "ctasystest"

# add all tapes to default tape pool
for ((i=0; i<${#TAPES[@]}; i++)); do
  VID=${TAPES[${i}]}
  kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tape add     \
    --mediatype "LTO8"                                                   \
    --purchaseorder "order"                                              \
    --vendor vendor                                                      \
    --logicallibrary ${TAPEDRIVES_IN_USE[${i}%${NB_TAPEDRIVES_IN_USE}]}  \
    --tapepool ctasystest                                                \
    --comment "ctasystest"                                               \
    --vid ${VID}                                                         \
    --full false                                                         \
    --comment "ctasystest"
done
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin mountpolicy add    \
  --name ctasystest                                                 \
  --archivepriority 1                                               \
  --minarchiverequestage 1                                          \
  --retrievepriority 1                                              \
  --minretrieverequestage 1                                         \
  --comment "ctasystest"
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin requestermountrule add \
    --instance ${DCACHE_INSTANCE_NAME}                                        \
    --name adm                                                       \
    --mountpolicy ctasystest --comment "ctasystest"

###
# This rule exists to allow users from dcacheusers group to migrate files to tapes
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin groupmountrule add \
    --instance ${DCACHE_INSTANCE_NAME}                                        \
    --name dcacheusers                                                  \
    --mountpolicy ctasystest --comment "ctasystest"
###
# This rule exists to allow users from powerusers group to recall files from tapes
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin groupmountrule add \
    --instance ${DCACHE_INSTANCE_NAME}                                        \
    --name powerusers                                                  \
    --mountpolicy ctasystest --comment "ctasystest"
###
# This mount policy is for repack: IT MUST CONTAIN THE `repack` STRING IN IT TO ALLOW MOUNTING DISABLED TAPES
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin mountpolicy add    \
    --name repack_ctasystest                                                 \
    --archivepriority 2                                               \
    --minarchiverequestage 1                                          \
    --retrievepriority 2                                              \
    --minretrieverequestage 1                                         \
    --comment "repack mountpolicy for ctasystest"

###
# This rule if for retrieves, and matches the retrieve activity used in the tests only
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin activitymountrule add \
    --instance ${DCACHE_INSTANCE_NAME}                                        \
    --name powerusers                                                \
    --activityregex ^T0Reprocess$                                    \
    --mountpolicy ctasystest --comment "ctasystest"

echo "Labeling tapes:"
# add all tapes
for VID in "${TAPES[@]}"; do
  echo "  cta-tape-label --vid ${VID} --force"
  # for debug use
  # kubectl --namespace ${NAMESPACE} exec ${CTA_TPSRV_POD} -c cta-taped-0  -- cta-tape-label --vid ${VID} --debug
  # The external tape format test leaves data inside of the tape, then the tapes for labeling are not empty between
  # tests. That's why we need to force cta-tape-label, but only for CI testing.
  kubectl --namespace ${NAMESPACE} exec ${CTA_TPSRV_POD} -c cta-taped-0  -- cta-tape-label --vid ${VID} --force
  if [[ $? -ne 0 ]]; then
    echo "ERROR: failed to label the tape ${VID}"
    exit 1
  fi
done

echo "Setting drive up: ${DRIVE_NAME}"
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin drive up ${DRIVE_NAME}
sleep 5
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin drive ls

# Super client capabilities
echo "Adding super client capabilities"
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin admin add --username ctaadmin2 --comment "ctaadmin2"

kubectl --namespace ${NAMESPACE} exec ${CLIENT_POD} -c client -- mkdir -p /tmp/ctaadmin2
kubectl --namespace ${NAMESPACE} exec ${CLIENT_POD} -c client -- mkdir -p /tmp/poweruser1
