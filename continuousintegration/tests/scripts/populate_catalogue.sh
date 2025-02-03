#!/bin/bash

# Exit on first failure
set -e

# Usage ./populate_catalogue.sh <disk instance name>

if [ "$#" -ne 1 ]; then
    echo "Please provide a diskinstance name"
    exit 1
fi
DISK_INSTANCE_NAME="$1"

cta-admin diskinstance add \
    --name "${DISK_INSTANCE_NAME}" \
    --comment "di"

cta-admin virtualorganization add \
    --vo vo \
    --readmaxdrives 1 \
    --writemaxdrives 1 \
    --diskinstance "${DISK_INSTANCE_NAME}" \
    --comment "vo"

cta-admin virtualorganization add \
    --vo vo_repack \
    --readmaxdrives 1 \
    --writemaxdrives 1 \
    --diskinstance "${DISK_INSTANCE_NAME}" \
    --comment "vo_repack" \
    --isrepackvo true

# add the media types of the tapes in production
cta-admin mediatype add \
    --name T10K500G \
    --capacity 500000000000 \
    --primarydensitycode 74 \
    --cartridge "T10000" \
    --comment "Oracle T10000 cartridge formated at 500 GB (for developers only)"
cta-admin mediatype add \
    --name 3592JC7T \
    --capacity 7000000000000 \
    --primarydensitycode 84 \
    --cartridge "3592JC" \
    --comment "IBM 3592JC cartridge formated at 7 TB"
cta-admin mediatype add \
    --name 3592JD15T \
    --capacity 15000000000000 \
    --primarydensitycode 85 \
    --cartridge "3592JD" \
    --comment "IBM 3592JD cartridge formated at 15 TB"
cta-admin mediatype add \
    --name 3592JE20T \
    --capacity 20000000000000 \
    --primarydensitycode 87 \
    --cartridge "3592JE" \
    --comment "IBM 3592JE cartridge formated at 20 TB"
cta-admin mediatype add \
    --name LTO7M \
    --capacity 9000000000000 \
    --primarydensitycode 93 \
    --cartridge "LTO-7" \
    --comment "LTO-7 M8 cartridge formated at 9 TB"
cta-admin mediatype add \
    --name LTO8 \
    --capacity 12000000000000 \
    --primarydensitycode 94 \
    --cartridge "LTO-8" \
    --comment "LTO-8 cartridge formated at 12 TB"

# Setup default tapepool and storage class
cta-admin tapepool add \
    --name ctasystest \
    --vo vo \
    --partialtapesnumber 5 \
    --encrypted false \
    --comment "ctasystest"

cta-admin storageclass add \
    --name ctaStorageClass \
    --numberofcopies 1 \
    --vo vo \
    --comment "ctasystest"
cta-admin archiveroute add \
    --storageclass ctaStorageClass \
    --copynb 1 \
    --tapepool ctasystest \
    --comment "ctasystest"

# Setup tapepools and storage classes for multiple tape copies
cta-admin tapepool add \
    --name ctasystest_A \
    --vo vo \
    --partialtapesnumber 5 \
    --encrypted false \
    --comment "ctasystest_A"
cta-admin tapepool add \
    --name ctasystest_B \
    --vo vo \
    --partialtapesnumber 5 \
    --encrypted false \
    --comment "ctasystest_B"
cta-admin tapepool add \
    --name ctasystest_C \
    --vo vo \
    --partialtapesnumber 5 \
    --encrypted false \
    --comment "ctasystest_C"

cta-admin storageclass add \
    --name ctaStorageClass_1_copy \
    --numberofcopies 1 \
    --vo vo \
    --comment "ctasystest"
cta-admin archiveroute add \
    --storageclass ctaStorageClass_1_copy \
    --copynb 1 \
    --tapepool ctasystest_A \
    --comment "ctasystest"

cta-admin storageclass add \
    --name ctaStorageClass_2_copy \
    --numberofcopies 2 \
    --vo vo \
    --comment "ctasystest"
cta-admin archiveroute add \
    --storageclass ctaStorageClass_2_copy \
    --copynb 1 \
    --tapepool ctasystest_A \
    --comment "ctasystest"
cta-admin archiveroute add \
    --storageclass ctaStorageClass_2_copy \
    --copynb 2 \
    --tapepool ctasystest_B \
    --comment "ctasystest"

cta-admin storageclass add \
    --name ctaStorageClass_3_copy \
    --numberofcopies 1 \
    --vo vo \
    --comment "ctasystest"
cta-admin archiveroute add \
    --storageclass ctaStorageClass_3_copy \
    --copynb 1 \
    --tapepool ctasystest_A \
    --comment "ctasystest"
cta-admin archiveroute add \
    --storageclass ctaStorageClass_3_copy \
    --copynb 2 \
    --tapepool ctasystest_B \
    --comment "ctasystest"
cta-admin archiveroute add \
    --storageclass ctaStorageClass_3_copy \
    --copynb 3 \
    --tapepool ctasystest_C \
    --comment "ctasystest"

cta-admin mountpolicy add \
    --name ctasystest \
    --archivepriority 1 \
    --minarchiverequestage 1 \
    --retrievepriority 1 \
    --minretrieverequestage 1 \
    --comment "ctasystest"

cta-admin requestermountrule add \
    --instance "${DISK_INSTANCE_NAME}" \
    --name adm \
    --mountpolicy ctasystest --comment "ctasystest"

###
# This rule exists to allow users from eosusers group to migrate files to tapes
cta-admin groupmountrule add \
    --instance "${DISK_INSTANCE_NAME}" \
    --name eosusers \
    --mountpolicy ctasystest --comment "ctasystest"
###
# This rule exists to allow users from powerusers group to recall files from tapes
cta-admin groupmountrule add \
    --instance "${DISK_INSTANCE_NAME}" \
    --name powerusers \
    --mountpolicy ctasystest --comment "ctasystest"
###
# This mount policy is for repack: IT MUST CONTAIN THE `repack` STRING IN IT TO ALLOW MOUNTING DISABLED TAPES
cta-admin mountpolicy add \
    --name repack_ctasystest \
    --archivepriority 2 \
    --minarchiverequestage 1 \
    --retrievepriority 2 \
    --minretrieverequestage 1 \
    --comment "repack mountpolicy for ctasystest"

###
# This rule if for retrieves, and matches the retrieve activity used in the tests only
cta-admin activitymountrule add \
    --instance "${DISK_INSTANCE_NAME}" \
    --name powerusers \
    --activityregex ^T0Reprocess$ \
    --mountpolicy ctasystest --comment "ctasystest"
