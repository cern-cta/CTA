#!/bin/bash

# Exit on first failure
set -e

# Usage ./populate_catalogue.sh <disk instance name>

if [ "$#" -ne 1 ]; then
    echo "Please provide a diskinstance name"
    exit 1
fi
DISK_INSTANCE_NAME="$1"
CTA_STORAGE_CLASS="ctaStorageClass"

# add the media types of the tapes in production
cta-admin mediatype add \
    --name 3592JC7T \
    --capacity 7000000000000 \
    --primarydensitycode 84 \
    --cartridge "3592JC" \
    --comment "IBM 3592JC cartridge formatted at 7 TB"
cta-admin mediatype add \
    --name 3592JD15T \
    --capacity 15000000000000 \
    --primarydensitycode 85 \
    --cartridge "3592JD" \
    --comment "IBM 3592JD cartridge formatted at 15 TB"
cta-admin mediatype add \
    --name 3592JE20T \
    --capacity 20000000000000 \
    --primarydensitycode 87 \
    --cartridge "3592JE" \
    --comment "IBM 3592JE cartridge formatted at 20 TB"
cta-admin mediatype add \
    --name 3592JF50T \
    --capacity 50000000000000 \
    --primarydensitycode 89 \
    --cartridge "3592JF" \
    --comment "IBM 3592JF cartridge formated at 50 TB"
cta-admin mediatype add \
    --name LTO7M \
    --capacity 9000000000000 \
    --primarydensitycode 93 \
    --cartridge "LTO-7" \
    --comment "LTO-7 M8 cartridge formatted at 9 TB"
cta-admin mediatype add \
    --name LTO8 \
    --capacity 12000000000000 \
    --primarydensitycode 94 \
    --cartridge "LTO-8" \
    --comment "LTO-8 cartridge formatted at 12 TB"
cta-admin mediatype add \
    --name LTO9 \
    --capacity 18000000000000 \
    --primarydensitycode 96 \
    --cartridge "LTO-9" \
    --comment "LTO-9 cartridge formatted at 18 TB"
cta-admin mediatype add \
    --name LTO10S \
    --capacity 30000000000000 \
    --primarydensitycode 98 \
    --cartridge "LTO-10" \
    --comment "LTO-10 standard cartridge formated at 30 TB"

# Add disk instance
# TODO: should some of this maybe move to the eos setup?

cta-admin diskinstance add \
    --name "${DISK_INSTANCE_NAME}" \
    --comment "Disk instance"

cta-admin virtualorganization add \
    --vo vo \
    --readmaxdrives 1 \
    --writemaxdrives 1 \
    --diskinstance "${DISK_INSTANCE_NAME}" \
    --comment "VO for system tests"

cta-admin storageclass add \
    --name "${CTA_STORAGE_CLASS}" \
    --numberofcopies 1 \
    --vo vo \
    --comment "ctasystest"

cta-admin tapepool add \
    --name ctasystest \
    --vo vo \
    --partialtapesnumber 5 \
    --comment "ctasystest"

cta-admin archiveroute add \
    --storageclass "${CTA_STORAGE_CLASS}" \
    --copynb 1 \
    --tapepool ctasystest \
    --comment "ctasystest"

# Setup mount policies
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
    --mountpolicy ctasystest \
    --comment "ctasystest"


# TODO: this is EOS specific
# This rule exists to allow users from eosusers group to archive files to tapes
cta-admin groupmountrule add \
    --instance "${DISK_INSTANCE_NAME}" \
    --name eosusers \
    --mountpolicy ctasystest \
    --comment "ctasystest"

# This rule exists to allow users from powerusers group to retrieve files from tapes
cta-admin groupmountrule add \
    --instance "${DISK_INSTANCE_NAME}" \
    --name powerusers \
    --mountpolicy ctasystest \
    --comment "ctasystest"

# This rule if for retrieves, and matches the retrieve activity used in the tests only
cta-admin activitymountrule add \
    --instance "${DISK_INSTANCE_NAME}" \
    --name powerusers \
    --activityregex ^T0Reprocess$ \
    --mountpolicy ctasystest --comment "ctasystest"
