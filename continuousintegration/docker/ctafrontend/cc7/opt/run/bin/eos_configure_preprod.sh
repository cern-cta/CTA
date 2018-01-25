#!/bin/bash
PREPROD_DIR=/eos/ctaeos/preprod
CTA_BIN=/usr/bin/eoscta_stub

mkdir -p /var/eos/wfe/bash
cp /opt/ci/ctaeos/var/eos/wfe/bash/* /var/eos/wfe/bash/
chmod 755 /var/eos/wfe/bash/*

eos mkdir ${PREPROD_DIR}
eos chmod 555 ${PREPROD_DIR}
eos attr set sys.acl=g:eosusers:rwx!d,u:poweruser1:rwx+dp,u:poweruser2:rwx+dp ${PREPROD_DIR}

eos attr set CTA_StorageClass=ctaStorageClass ${PREPROD_DIR}

eos attr set CTA_TapeFsId=65535 ${PREPROD_DIR}

eos attr set sys.workflow.closew.default="proto/cta:ctafrontend:10955 <parent/file>" ${PREPROD_DIR}

#eos attr set sys.workflow.archived.default="bash:shell:cta eos file tag <eos::wfe::path> +<eos::wfe::cxattr:CTA_TapeFsId>" ${PREPROD_DIR}
eos attr set sys.workflow.archived.default="bash:create_tape_drop_disk_replicas:cta <eos::wfe::path> <eos::wfe::cxattr:CTA_TapeFsId>" ${PREPROD_DIR}

eos attr set sys.workflow.sync::prepare.default="proto/cta:ctafrontend:10955 <parent/file>" ${PREPROD_DIR}

eos attr set sys.workflow.closew.CTA_retrieve="bash:shell:cta eos attr set 'CTA_retrieved_timestamp=\"\`date\`\"' <eos::wfe::path>" ${PREPROD_DIR}

eos attr set sys.workflow.sync::delete.default="proto/cta:ctafrontend:10955 <parent/file>" ${PREPROD_DIR}
