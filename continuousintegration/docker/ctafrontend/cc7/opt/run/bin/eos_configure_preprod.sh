#!/bin/bash
PREPROD_DIR=/eos/ctaeos/preprod

eos mkdir ${PREPROD_DIR}
eos chmod 555 ${PREPROD_DIR}
eos attr set sys.acl=g:eosusers:rwx!d,u:poweruser1:rwx+dp,u:poweruser2:rwx+dp ${PREPROD_DIR}

eos attr set sys.archive.storage_class=ctaStorageClass ${PREPROD_DIR}

eos attr set sys.workflow.sync::create.default="proto" ${PREPROD_DIR}
eos attr set sys.workflow.sync::closew.default="proto" ${PREPROD_DIR}
eos attr set sys.workflow.sync::archived.default="proto" ${PREPROD_DIR}
eos attr set sys.workflow.sync::archive_failed.default="proto" ${PREPROD_DIR}
eos attr set sys.workflow.sync::prepare.default="proto" ${PREPROD_DIR}
eos attr set sys.workflow.sync::abort_prepare.default="proto" ${PREPROD_DIR}
eos attr set sys.workflow.sync::evict_prepare.default="proto" ${PREPROD_DIR}
eos attr set sys.workflow.sync::closew.retrieve_written="proto" ${PREPROD_DIR}
eos attr set sys.workflow.sync::retrieve_failed.default="proto" ${PREPROD_DIR}
eos attr set sys.workflow.sync::delete.default="proto" ${PREPROD_DIR}
