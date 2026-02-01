#!/usr/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# This script is meant to be executed on the EOS MGM

# General settings
eos vid enable unix
eos vid enable https
eos space set default on
eos space config default space.filearchivedgc=on
eos space config default space.wfe=on
eos space config default space.wfe.ntx=500
eos space config default space.wfe.interval=1
eos space config default taperestapi.status=on
eos space config default taperestapi.stage=on
eos space config default space.scanrate=0
eos space config default space.scaninterval=0
eos space config default space.token.generation=1
eos space config default space.scanrate=0
eos space config default space.scaninterval=0
eos attr -r set default=replica /eos
eos attr -r set sys.forced.nstripes=1 /eos
eos debug err "*"

# Users
groupadd --gid 1100 eosusers
groupadd --gid 1200 powerusers
groupadd --gid 1300 ctaadmins
groupadd --gid 1400 eosadmins
useradd --uid 11001 --gid 1100 user1
useradd --uid 11002 --gid 1100 user2
useradd --uid 12001 --gid 1200 poweruser1
useradd --uid 12002 --gid 1200 poweruser2
useradd --uid 13001 --gid 1300 ctaadmin1
useradd --uid 13002 --gid 1300 ctaadmin2
useradd --uid 14001 --gid 1400 eosadmin1
useradd --uid 14002 --gid 1400 eosadmin2

eos vid set membership "$(id -u eosadmin1)" +sudo
eos vid set membership "$(id -u eosadmin2)" +sudo


EOS_INSTANCE_NAME=ctaeos
CTA_STORAGE_CLASS=ctaStorageClass
CTA_PROC_DIR=/eos/${EOS_INSTANCE_NAME}/proc/cta
CTA_WF_DIR=${CTA_PROC_DIR}/workflow
EOS_TMP_DIR=/eos/${EOS_INSTANCE_NAME}/tmp

# Test specific

TAPE_FS_ID=65535
eos space define tape
eos fs add -m ${TAPE_FS_ID} tape localhost:1234 /does_not_exist tape

# create tmp disk only directory for tests
eos mkdir ${EOS_TMP_DIR}
eos chmod 777 ${EOS_TMP_DIR}

# Configure directories

eos mkdir -p ${CTA_PROC_DIR}
eos mkdir -p ${CTA_WF_DIR}

eos attr set sys.workflow.sync::create.default="proto" ${CTA_WF_DIR}
eos attr set sys.workflow.sync::closew.default="proto" ${CTA_WF_DIR}
eos attr set sys.workflow.sync::archived.default="proto" ${CTA_WF_DIR}
eos attr set sys.workflow.sync::archive_failed.default="proto" ${CTA_WF_DIR}
eos attr set sys.workflow.sync::prepare.default="proto" ${CTA_WF_DIR}
eos attr set sys.workflow.sync::abort_prepare.default="proto" ${CTA_WF_DIR}
eos attr set sys.workflow.sync::evict_prepare.default="proto" ${CTA_WF_DIR}
eos attr set sys.workflow.sync::closew.retrieve_written="proto" ${CTA_WF_DIR}
eos attr set sys.workflow.sync::retrieve_failed.default="proto" ${CTA_WF_DIR}
eos attr set sys.workflow.sync::delete.default="proto" ${CTA_WF_DIR}

# ${CTA_PREPROD_DIR} must be writable by eosusers and powerusers
# but as there is no sticky bit in eos, we need to remove deletion for non owner to eosusers members
# this is achieved through the ACLs.
# ACLs in EOS are evaluated when unix permissions are failing, hence the 555 unix permission.
CTA_PREPROD_DIR=/eos/${EOS_INSTANCE_NAME}/preprod
eos mkdir -p ${CTA_PREPROD_DIR}
eos chmod 555 ${CTA_PREPROD_DIR}
eos attr set sys.acl=g:eosusers:rwx!d,u:poweruser1:rwx+dp,u:poweruser2:rwx+dp,z:'!'u'!'d ${CTA_PREPROD_DIR}
eos attr set sys.archive.storage_class=${CTA_STORAGE_CLASS} ${CTA_PREPROD_DIR}
eos attr link ${CTA_WF_DIR} ${CTA_PREPROD_DIR} # Link workflows

# ${CTA_TEST_DIR} must be writable by eosusers and powerusers
# but as there is no sticky bit in eos, we need to remove deletion for non owner to eosusers members
# this is achieved through the ACLs.
# ACLs in EOS are evaluated when unix permissions are failing, hence the 555 unix permission.
CTA_TEST_DIR=/eos/${EOS_INSTANCE_NAME}/cta
eos mkdir ${CTA_TEST_DIR}
eos chmod 555 ${CTA_TEST_DIR}
eos attr set sys.acl=g:eosusers:rwx!d,u:poweruser1:rwx+dp,u:poweruser2:rwx+dp,z:'!'u'!'d ${CTA_TEST_DIR}
eos attr set sys.archive.storage_class=ctaStorageClass ${CTA_TEST_DIR}
eos attr link ${CTA_WF_DIR} ${CTA_TEST_DIR} # Link workflows
# ${CTA_TEST_NO_P_DIR} must be writable by eosusers and powerusers
# but not allow prepare requests.
# this is achieved through the ACLs.
# This directory is created inside ${CTA_TEST_DIR}.
CTA_TEST_NO_P_DIR=${CTA_TEST_DIR}/no_prepare
eos mkdir ${CTA_TEST_NO_P_DIR}
eos attr set sys.acl=g:eosusers:rwx!d,u:poweruser1:rwx+d,u:poweruser2:rwx+d,z:'!'u'!'d ${CTA_TEST_NO_P_DIR}
