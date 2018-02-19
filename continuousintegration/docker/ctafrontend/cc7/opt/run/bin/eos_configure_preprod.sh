#!/bin/bash
PREPROD_DIR=/eos/ctaeos/preprod

eos mkdir ${PREPROD_DIR}
eos chmod 555 ${PREPROD_DIR}
eos attr set sys.acl=g:eosusers:rwx!d,u:poweruser1:rwx+dp,u:poweruser2:rwx+dp ${PREPROD_DIR}

eos attr set CTA_StorageClass=ctaStorageClass ${PREPROD_DIR}
eos attr set CTA_TapeFsId=65535 ${PREPROD_DIR}

eos attr set sys.workflow.sync::create.default="proto:ctafrontend:10955 ctafrontend" ${PREPROD_DIR}
eos attr set sys.workflow.closew.default="proto:ctafrontend:10955 ctafrontend" ${PREPROD_DIR}
eos attr set sys.workflow.archived.default="proto:ctafrontend:10955 ctafrontend" ${PREPROD_DIR}
eos attr set sys.workflow.sync::prepare.default="proto:ctafrontend:10955 ctafrontend" ${PREPROD_DIR}
eos attr set sys.workflow.sync::delete.default="proto:ctafrontend:10955 ctafrontend" ${PREPROD_DIR}
