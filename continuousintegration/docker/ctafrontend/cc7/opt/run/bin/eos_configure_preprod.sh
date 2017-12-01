#!/bin/bash
PREPROD_DIR=/eos/ctaeos/preprod
CTA_BIN=/usr/bin/eoscta_stub

mkdir -p /var/eos/wfe/bash
cp /opt/ci/ctaeos/var/eos/wfe/bash/* /var/eos/wfe/bash/
chmod 755 /var/eos/wfe/bash/*

eos mkdir ${PREPROD_DIR}
eos chmod 555 ${PREPROD_DIR}
eos attr set sys.acl=g:eosusers:rwx!d,g:powerusers:rwx+dp ${PREPROD_DIR}

eos attr set CTA_StorageClass=ctaStorageClass ${PREPROD_DIR}

eos attr set CTA_TapeFsId=65535 ${PREPROD_DIR}

eos attr set sys.workflow.closew.default="bash:shell:cta XrdSecPROTOCOL=sss XrdSecSSSKT=/etc/cta/cta-cli.sss.keytab ${CTA_BIN} archive --user <eos::wfe::rusername> --group <eos::wfe::rgroupname> --diskid <eos::wfe::fid> --instance ignored_instance_name --srcurl <eos::wfe::turl> --size <eos::wfe::size> --checksumtype <eos::wfe::checksumtype> --checksumvalue <eos::wfe::checksum> --storageclass <eos::wfe::cxattr:CTA_StorageClass> --diskfilepath <eos::wfe::path> --diskfileowner <eos::wfe::username> --diskfilegroup <eos::wfe::groupname> --recoveryblob:base64 cmVjb3ZlcnkK --reportURL 'eosQuery://ctaeos//eos/wfe/passwd?mgm.pcmd=event\&mgm.fid=<eos::wfe::fxid>\&mgm.logid=cta\&mgm.event=archived\&mgm.workflow=default\&mgm.path=/eos/wfe/passwd\&mgm.ruid=0\&mgm.rgid=0' --stderr" ${PREPROD_DIR}

#eos attr set sys.workflow.archived.default="bash:shell:cta eos file tag <eos::wfe::path> +<eos::wfe::cxattr:CTA_TapeFsId>" ${PREPROD_DIR}
eos attr set sys.workflow.archived.default="bash:create_tape_drop_disk_replicas:cta <eos::wfe::path> <eos::wfe::cxattr:CTA_TapeFsId>" ${PREPROD_DIR}

eos attr set sys.workflow.sync::prepare.default="bash:retrieve_archive_file:cta <eos::wfe::rusername> <eos::wfe::rgroupname> <eos::wfe::fxattr:sys.archiveFileId> <eos::wfe::turl> <eos::wfe::username> <eos::wfe::groupname> cmVjb3ZlcnkK <eos::wfe::path>" ${PREPROD_DIR}

eos attr set sys.workflow.closew.CTA_retrieve="bash:shell:cta eos attr set 'CTA_retrieved_timestamp=\"\`date\`\"' <eos::wfe::path>" ${PREPROD_DIR}

eos attr set sys.workflow.sync::delete.default="bash:delete_archive_file:cta <eos::wfe::rusername> <eos::wfe::rgroupname> <eos::wfe::fxattr:sys.archiveFileId> <eos::wfe::path>" ${PREPROD_DIR}
