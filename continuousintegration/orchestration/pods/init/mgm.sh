#!/bin/sh 

# initialize pod
/shared/bin/init_pod.sh

eoshost=`hostname -f`
eosInstanceName=`hostname -s`

TAPE_FS_ID=65535
CTA_BIN=/usr/bin/cta
CTA_KT=/etc/eoscta-cli.keytab
CTA_PROC_DIR=/eos/${eosInstanceName}/proc/cta
CTA_WF_DIR=${CTA_PROC_DIR}/workflow
CTA_TEST_DIR=/eos/${eosInstanceName}/cta

# prepare CTA cli commands environment
  echo ${frontend} > /etc/cta/cta-cli.conf

# setup eos host and instance name
  sed -i -e "s/DUMMY_HOST_TO_REPLACE/${eoshost}/" /etc/sysconfig/eos
  sed -i -e "s/DUMMY_INSTANCE_TO_REPLACE/${eosInstanceName}/" /etc/sysconfig/eos
  sed -i -e "s/DUMMY_HOST_TO_REPLACE/${eoshost}/" /etc/xrd.cf.mgm
  sed -i -e "s/DUMMY_INSTANCE_TO_REPLACE/${eosInstanceName}/" /etc/xrd.cf.mgm
  sed -i -e "s/DUMMY_HOST_TO_REPLACE/${eoshost}/" /etc/xrd.cf.mq
  sed -i -e "s/DUMMY_HOST_TO_REPLACE/${eoshost}/" /etc/xrd.cf.fst

# prepare eos startup
  # skip systemd for eos initscripts
    export SYSTEMCTL_SKIP_REDIRECT=1
  echo y | xrdsssadmin -k ${eosInstanceName} -u daemon -g daemon add /etc/eos.keytab
    chmod 400 /etc/eos.keytab
    chown daemon:daemon /etc/eos.keytab
  mkdir -p /run/lock/subsys
  mkdir -p /var/eos/config/${eoshost}
    chown daemon:root /var/eos/config/${eoshost}
  touch   /var/eos/config/${eoshost}/default.eoscf
    chown daemon:daemon /var/eos/config/${eoshost}/default.eoscf

  #/etc/init.d/eos master mgm
  #/etc/init.d/eos master mq
    touch /var/eos/eos.mq.master
    touch /var/eos/eos.mgm.rw
    echo "Configured mq mgm on localhost as master"

  source /etc/sysconfig/eos

  mkdir -p /fst
  chown daemon:daemon /fst/

# start and setup eos for xrdcp to the ${CTA_TEST_DIR}
  #/etc/init.d/eos start
    /usr/bin/xrootd -n mq -c /etc/xrd.cf.mq -l /var/log/eos/xrdlog.mq -b -Rdaemon
    /usr/bin/xrootd -n mgm -c /etc/xrd.cf.mgm -m -l /var/log/eos/xrdlog.mgm -b -Rdaemon
    /usr/bin/xrootd -n fst -c /etc/xrd.cf.fst -l /var/log/eos/xrdlog.fst -b -Rdaemon

  eos vid enable sss
  eos vid enable unix
  EOS_MGM_URL="root://${eoshost}" eosfstregister -r /fst default:1

  eos node set ${eoshost} on
  eos space set default on
  eos attr -r set default=replica /eos
  eos attr -r set sys.forced.nstripes=1 /eos

  eos fs add -m ${TAPE_FS_ID} tape localhost:1234 /does_not_exist tape
  eos mkdir ${CTA_PROC_DIR}
  eos mkdir ${CTA_WF_DIR}
  eos attr set CTA_TapeFsId=${TAPE_FS_ID} ${CTA_WF_DIR}
  
  eos mkdir ${CTA_TEST_DIR}
  eos chmod 777 ${CTA_TEST_DIR}
  eos attr set CTA_StorageClass=ctaStorageClass ${CTA_TEST_DIR}
    
  # hack before it is fixed in EOS
    TAPE_FS_ID_TOSET=`eos attr ls ${CTA_WF_DIR} | grep CTA_TapeFsId= | tr '"' ' ' | cut -d ' ' -f 2`
    eos attr set CTA_TapeFsId=${TAPE_FS_ID_TOSET} ${CTA_TEST_DIR}

  # Link the attributes of CTA worklow directory to the test directory
  eos attr link ${CTA_WF_DIR} ${CTA_TEST_DIR}

# test EOS
# eos slow behind us and we need to give it time to be ready
# 5 secs is not enough
  sleep 10
  eos -b node ls
  xrdcp /etc/group root://${eoshost}:/${CTA_TEST_DIR}/testFile

# prepare EOS workflow
  eos space config default space.wfe=on

  # ATTENTION
  # for sss authorisation  unix has to be replaced by sss

  # Set the worfklow rule for archiving files to tape
    eos attr set sys.workflow.closew.default="bash:shell:cta XrdSecPROTOCOL=unix XrdSecSSSKT=${CTA_KT} ${CTA_BIN} archive --user <eos::wfe::rusername> --group <eos::wfe::rgroupname> --diskid <eos::wfe::fid> --instance eoscta --srcurl <eos::wfe::turl> --size <eos::wfe::size> --checksumtype <eos::wfe::checksumtype> --checksumvalue <eos::wfe::checksum> --storageclass <eos::wfe::cxattr:CTA_StorageClass> --diskfilepath <eos::wfe::path> --diskfileowner <eos::wfe::username> --diskfilegroup <eos::wfe::groupname> --recoveryblob:base64 <eos::wfe::base64:metadata> --diskpool default --throughput 10000 --stderr" ${CTA_WF_DIR}
  # Set the worflow rule for creating tape file replicas in the EOS namespace
    eos attr set sys.workflow.archived.default="bash:shell:cta eos file tag <eos::wfe::path> +<eos::wfe::cxattr:CTA_TapeFsId>" ${CTA_WF_DIR}
  # Set the worfklow rule for retrieving file from tape
    eos attr set sys.workflow.prepare.default="bash:shell:cta XrdSecPROTOCOL=unix XrdSecSSSKT=${CTA_KT} ${CTA_BIN} retrieve --user <eos::wfe::rusername> --group <eos::wfe::rgroupname> --id <eos::wfe::fxattr:sys.archiveFileId> --dsturl '<eos::wfe::turl>\&eos.injection=1\&eos.workflow=CTA_retrieve' --diskfilepath <eos::wfe::path> --diskfileowner <eos::wfe::username> --diskfilegroup <eos::wfe::groupname> --recoveryblob:base64 <eos::wfe::base64:metadata> --diskpool default --throughput 10001 --stderr" ${CTA_WF_DIR}
  # Set the workflow rule for the closew event of the CTA_retrieve workflow.
  # Using the CTA_retrieve workflow will prevent the default workflow from
  # receiving the closew event.  Triggering the default workflow in this way would
  # haved causes the unwanted action of copying the disk file to tape again.
  # The action of the CTA_retrieve workflow when triggered by the closew event is
  # to set the CTA_retrieved_timestamp attribute.
    eos attr set sys.workflow.closew.CTA_retrieve="bash:shell:cta eos attr set 'CTA_retrieved_timestamp=\"\`date\`\"' <eos::wfe::path>" ${CTA_WF_DIR}

touch /shared/${INSTANCE_NAME}/mgm_ready

/bin/bash
