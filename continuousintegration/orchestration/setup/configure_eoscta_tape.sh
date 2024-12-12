#!/usr/bin/bash

# This script is meant to be executed on the EOS MGM

# General settings
eos vid enable unix
eos space set default on
eos attr -r set default=replica /eos
eos attr -r set sys.forced.nstripes=1 /eos
eos space config default space.wfe=on
eos space config default space.wfe.ntx=200
eos space config default space.filearchivedgc=on
# TODO: move to specific test
eos space config default space.token.generation=1

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

eos vid set membership $(id -u eosadmin1) +sudo
eos vid set membership $(id -u eosadmin2) +sudo

# Configure tape

DESTDIR=preprod
CTA_STORAGE_CLASS=ctaStorageClass
EOS_INSTANCE=ctaeos

CTA_PROC_DIR=/eos/${EOS_INSTANCE}/proc/cta
CTA_WF_DIR=${CTA_PROC_DIR}/workflow$(echo ${DESTDIR} | sed -e 's%/%_%g')
CTA_DEST_DIR=/eos/${EOS_INSTANCE}/${DESTDIR}

eos mkdir -p ${CTA_PROC_DIR}
eos mkdir -p ${CTA_WF_DIR}

for WORKFLOW in sync::create.default sync::closew.default sync::archived.default sync::archive_failed.default sync::prepare.default sync::abort_prepare.default sync::evict_prepare.default sync::closew.retrieve_written sync::retrieve_failed.default sync::delete.default
do
  eos attr set sys.workflow.${WORKFLOW}=\"proto\" ${CTA_WF_DIR}
done

# ${CTA_DEST_DIR} must be writable by eosusers and powerusers
# but as there is no sticky bit in eos, we need to remove deletion for non owner to eosusers members
# this is achieved through the ACLs.
# ACLs in EOS are evaluated when unix permissions are failing, hence the 555 unix permission.
eos mkdir -p ${CTA_DEST_DIR}
eos chmod 555 ${CTA_DEST_DIR}
eos attr set sys.acl=g:eosusers:rwx!d,u:poweruser1:rwx+dp,u:poweruser2:rwx+dp,z:'!'u'!'d ${CTA_DEST_DIR}
eos attr set sys.archive.storage_class=${CTA_STORAGE_CLASS} ${CTA_DEST_DIR}

# Link the attributes of CTA worklow directory to the destination directory
eos attr link ${CTA_WF_DIR} ${CTA_DEST_DIR}
