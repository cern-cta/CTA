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
# eos instance identified by SSS username
EOS_MGM_POD="eos-mgm-0"
EOS_INSTANCE_NAME="ctaeos"

MULTICOPY_DIR_1=/eos/ctaeos/preprod/dir_1_copy
MULTICOPY_DIR_2=/eos/ctaeos/preprod/dir_2_copy
MULTICOPY_DIR_3=/eos/ctaeos/preprod/dir_3_copy

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
  TAPEDRIVES_IN_USE+=($(kubectl --namespace ${NAMESPACE} exec ${tapeserver} -c cta-taped-0 -- bash -c "find /etc/cta | grep cta-taped.conf | xargs cat" | grep LogicalLibrary | awk 'NR==1 {print $3}'))
done
NB_TAPEDRIVES_IN_USE=${#TAPEDRIVES_IN_USE[@]}

echo "Preparing CTA configuration for tests"
# verify the catalogue DB schema
if ! kubectl --namespace ${NAMESPACE} exec ${CTA_FRONTEND_POD} -c cta-frontend -- cta-catalogue-schema-verify /etc/cta/cta-catalogue.conf; then
  echo "ERROR: failed to verify the catalogue DB schema"
  exit 1
fi
kubectl --namespace ${NAMESPACE} exec ${CTA_FRONTEND_POD} -c cta-frontend -- cta-catalogue-admin-user-create /etc/cta/cta-catalogue.conf --username ctaadmin1 -m "docker cli"

# If using GRPC with JWT authentication, obtain JWT token in cli and client pod
if kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- printenv GRPC_SETUP_JWT 2>/dev/null | grep -q true; then
  echo "Detected GRPC JWT authentication for cli pod, obtaining JWT..."
  kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- /root/grpc_obtain_jwt.sh || {
    echo "ERROR: Failed to obtain JWT token for GRPC authentication in cli pod"
    exit 1
  }
fi

if kubectl --namespace ${NAMESPACE} exec ${CLIENT_POD} -c client -- printenv GRPC_SETUP_JWT 2>/dev/null | grep -q true; then
  echo "Detected GRPC JWT authentication for client pod, obtaining JWT..."
  kubectl --namespace ${NAMESPACE} exec ${CLIENT_POD} -c client -- /root/grpc_obtain_jwt.sh || {
    echo "ERROR: Failed to obtain JWT token for GRPC authentication in client pod"
    exit 1
  }
fi

echo 'kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json version | jq'
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json version | jq

echo "Cleaning up leftovers from potential previous runs."
kubectl --namespace ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos rm -rf /eos/ctaeos/cta/fail_on_closew_test/ 2>/dev/null
kubectl --namespace ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos rm -rf /eos/ctaeos/cta/* 2>/dev/null
kubectl --namespace ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos rm -rf ${MULTICOPY_DIR_1}/ 2>/dev/null
kubectl --namespace ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos rm -rf ${MULTICOPY_DIR_2}/ 2>/dev/null
kubectl --namespace ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos rm -rf ${MULTICOPY_DIR_3}/ 2>/dev/null
kubectl --namespace ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos find -f /eos/ctaeos/preprod/ | xargs -I{} kubectl --namespace ${NAMESPACE} exec ${EOS_MGM_POD} -- eos rm -rf {} 2>/dev/null
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json tape ls --all  |             \
  jq -r '.[] | .vid ' | xargs -I{} kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli --            \
  cta-admin tape rm -v {}

kubectl --namespace ${NAMESPACE}  exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json archiveroute ls |           \
  jq '.[] |  " -s " + .storageClass + " -c " + (.copyNumber|tostring) + " --art " + .archiveRouteType' | \
  xargs -I{} bash -c "kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin archiveroute rm {}"

kubectl --namespace ${NAMESPACE}  exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json tapepool ls  |              \
  jq -r '.[] | .name' |                                                                       \
  xargs -I{} kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tapepool rm -n {}

kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json storageclass ls  |           \
  jq -r '.[] | " -n  " + .name'  |                                    \
  xargs -I{} bash -c "kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin storageclass rm {}"

kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json vo ls  |           \
  jq -r '.[] | " --vo  " + .name'  |                                    \
  xargs -I{} bash -c "kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin vo rm {}"


for ((i=0; i<${#TAPEDRIVES_IN_USE[@]}; i++)); do
  kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin logicallibrary add \
    --name ${TAPEDRIVES_IN_USE[${i}]}                                            \
    --comment "ctasystest library mapped to drive ${TAPEDRIVES_IN_USE[${i}]}"
done

kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin diskinstance add  \
  --name ${EOS_INSTANCE_NAME}                                                    \
  --comment "di"

kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin virtualorganization add  \
  --vo vo                                                                          \
  --readmaxdrives 1                                                                \
  --writemaxdrives 1                                                               \
  --diskinstance ${EOS_INSTANCE_NAME}                                                    \
  --comment "vo"

kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin virtualorganization add  \
  --vo vo_repack                                                                   \
  --readmaxdrives 1                                                                \
  --writemaxdrives 1                                                               \
  --diskinstance ${EOS_INSTANCE_NAME}                                                    \
  --comment "vo_repack"                                                            \
  --isrepackvo true

# add the media types of the tapes in production
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
  --name ctaStorageClass                                                   \
  --numberofcopies 1                                                       \
  --vo vo                                                                  \
  --comment "ctasystest"
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin archiveroute add \
  --storageclass ctaStorageClass                                           \
  --copynb 1                                                               \
  --tapepool ctasystest                                                    \
  --comment "ctasystest"

# Setup tapepools and storage classes for multiple tape copies
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tapepool add \
  --name ctasystest_A                                                  \
  --vo vo                                                              \
  --partialtapesnumber 5                                               \
  --comment "ctasystest_A"
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tapepool add \
  --name ctasystest_B                                                  \
  --vo vo                                                              \
  --partialtapesnumber 5                                               \
  --comment "ctasystest_B"
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tapepool add \
  --name ctasystest_C                                                  \
  --vo vo                                                              \
  --partialtapesnumber 5                                               \
  --comment "ctasystest_C"

kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin storageclass add \
  --name ctaStorageClass_1_copy                                            \
  --numberofcopies 1                                                       \
  --vo vo                                                                  \
  --comment "ctasystest"
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin archiveroute add \
  --storageclass ctaStorageClass_1_copy                                    \
  --copynb 1                                                               \
  --tapepool ctasystest_A                                                  \
  --comment "ctasystest"

kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin storageclass add \
  --name ctaStorageClass_2_copy                                            \
  --numberofcopies 2                                                       \
  --vo vo                                                                  \
  --comment "ctasystest"
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin archiveroute add \
  --storageclass ctaStorageClass_2_copy                                    \
  --copynb 1                                                               \
  --tapepool ctasystest_A                                                  \
  --comment "ctasystest"
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin archiveroute add \
  --storageclass ctaStorageClass_2_copy                                    \
  --copynb 2                                                               \
  --tapepool ctasystest_B                                                  \
  --comment "ctasystest"

kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin storageclass add \
  --name ctaStorageClass_3_copy                                            \
  --numberofcopies 1                                                       \
  --vo vo                                                                  \
  --comment "ctasystest"
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin archiveroute add \
  --storageclass ctaStorageClass_3_copy                                    \
  --copynb 1                                                               \
  --tapepool ctasystest_A                                                  \
  --comment "ctasystest"
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin archiveroute add \
  --storageclass ctaStorageClass_3_copy                                    \
  --copynb 2                                                               \
  --tapepool ctasystest_B                                                  \
  --comment "ctasystest"
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin archiveroute add \
  --storageclass ctaStorageClass_3_copy                                    \
  --copynb 3                                                               \
  --tapepool ctasystest_C                                                  \
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
    --instance ${EOS_INSTANCE_NAME}                                        \
    --name adm                                                       \
    --mountpolicy ctasystest --comment "ctasystest"

###
# This rule exists to allow users from eosusers group to migrate files to tapes
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin groupmountrule add \
    --instance ${EOS_INSTANCE_NAME}                                        \
    --name eosusers                                                  \
    --mountpolicy ctasystest --comment "ctasystest"
###
# This rule exists to allow users from powerusers group to recall files from tapes
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin groupmountrule add \
    --instance ${EOS_INSTANCE_NAME}                                        \
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
    --instance ${EOS_INSTANCE_NAME}                                        \
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

# A bit of reporting
echo "EOS server version is used:"
kubectl --namespace ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- rpm -qa | grep eos-server


# Super client capabilities
echo "Adding super client capabilities"
kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin admin add --username ctaadmin2 --comment "ctaadmin2"

kubectl --namespace ${NAMESPACE} exec ${CLIENT_POD} -c client -- mkdir -p /tmp/ctaadmin2
kubectl --namespace ${NAMESPACE} exec ${CLIENT_POD} -c client -- mkdir -p /tmp/poweruser1
kubectl --namespace ${NAMESPACE} exec ${CLIENT_POD} -c client -- mkdir -p /tmp/eosadmin1

setup_tapes_for_multicopy_test() {

  echo "Setting up tapes and tapepools for multi-copy test..."

  kubectl --namespace ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos mkdir ${MULTICOPY_DIR_1}
  kubectl --namespace ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos mkdir ${MULTICOPY_DIR_2}
  kubectl --namespace ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos mkdir ${MULTICOPY_DIR_3}

  kubectl --namespace ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos attr set sys.archive.storage_class=ctaStorageClass_1_copy ${MULTICOPY_DIR_1}
  kubectl --namespace ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos attr set sys.archive.storage_class=ctaStorageClass_2_copy ${MULTICOPY_DIR_2}
  kubectl --namespace ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos attr set sys.archive.storage_class=ctaStorageClass_3_copy ${MULTICOPY_DIR_3}

  # Find 3 non-full tapes and assign them to each one of the 3 tapepools
  mapfile -t nonFullTapes < <( kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json tape ls --all | jq -r '.[] | select(.full==false) | .vid' )
  if ((${#nonFullTapes[@]} < 3)); then
    echo "Not enought non-full tapes"
    return 1
  fi

  kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tape ch --vid ${nonFullTapes[0]} --tapepool ctasystest_A
  kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tape ch --vid ${nonFullTapes[1]} --tapepool ctasystest_B
  kubectl --namespace ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tape ch --vid ${nonFullTapes[2]} --tapepool ctasystest_C
}
