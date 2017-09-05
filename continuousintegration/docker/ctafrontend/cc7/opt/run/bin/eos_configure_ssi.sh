#!/bin/bash

######
#
# Setup an EOS folder with SSI workflows
# until EOS is doing SSI on its own
#
######

SSI_DIR=/eos/ctaeos/preprod
CTA_BIN=/usr/bin/eoscta_stub

mkdir -p /var/eos/wfe/bash
cp /opt/ci/ctaeos/var/eos/wfe/bash/* /var/eos/wfe/bash/
chmod 755 /var/eos/wfe/bash/*

eos mkdir ${SSI_DIR}
eos chmod 555 ${SSI_DIR}
eos attr set sys.acl=g:eosusers:rwx!d,g:powerusers:rwx+d ${SSI_DIR}

eos attr set CTA_StorageClass=ctaStorageClass ${SSI_DIR}

eos attr set CTA_TapeFsId=65535 ${SSI_DIR}

eos attr set sys.workflow.closew.default="bash:shell:cta XrdSecPROTOCOL=sss XrdSecSSSKT=/etc/ctafrontend_SSS_c.keytab ${CTA_BIN} archive --user <eos::wfe::rusername> --group <eos::wfe::rgroupname> --diskid <eos::wfe::fid> --instance eoscta --srcurl <eos::wfe::turl> --size <eos::wfe::size> --checksumtype <eos::wfe::checksumtype> --checksumvalue <eos::wfe::checksum> --storageclass <eos::wfe::cxattr:CTA_StorageClass> --diskfilepath <eos::wfe::path> --diskfileowner <eos::wfe::username> --diskfilegroup <eos::wfe::groupname> --recoveryblob:base64 <eos::wfe::base64:metadata> --reportURL 'eosQuery://ctaeos//eos/wfe/passwd?mgm.pcmd=event\&mgm.fid=<eos::wfe::fxid>\&mgm.logid=cta\&mgm.event=archived\&mgm.workflow=default\&mgm.path=/eos/wfe/passwd\&mgm.ruid=0\&mgm.rgid=0' --stderr" ${SSI_DIR}

#eos attr set sys.workflow.archived.default="bash:shell:cta eos file tag <eos::wfe::path> +<eos::wfe::cxattr:CTA_TapeFsId>" ${SSI_DIR}
eos attr set sys.workflow.archived.default="bash:create_tape_drop_disk_replicas_ssi:cta <eos::wfe::path> <eos::wfe::cxattr:CTA_TapeFsId>" ${SSI_DIR}


eos attr set sys.workflow.sync::prepare.default="bash:retrieve_archive_file_ssi:cta <eos::wfe::rusername> <eos::wfe::rgroupname> <eos::wfe::fxattr:sys.archiveFileId> <eos::wfe::turl> <eos::wfe::username> <eos::wfe::groupname> <eos::wfe::base64:metadata> <eos::wfe::path>" ${SSI_DIR}

eos attr set sys.workflow.closew.CTA_retrieve="bash:shell:cta eos attr set 'CTA_retrieved_timestamp=\"\`date\`\"' <eos::wfe::path>" ${SSI_DIR}

eos attr set sys.workflow.sync::delete.default="bash:delete_archive_file_ssi:cta <eos::wfe::rusername> <eos::wfe::rgroupname> <eos::wfe::fxattr:sys.archiveFileId> <eos::wfe::path>" ${SSI_DIR}
