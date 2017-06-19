#!/bin/sh 

/opt/run/bin/init_pod.sh

yum-config-manager --enable cta-artifacts
yum-config-manager --enable eos-citrine-commit
yum-config-manager --enable eos-citrine-depend
yum-config-manager --enable eos-citrine

# Install missing RPMs
yum -y install eos-client eos-server xrootd-client xrootd-debuginfo xrootd-server cta-cli cta-debuginfo

# create local users as the mgm is the only one doing the uid/user/group mapping in the full infrastructure
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

# copy needed template configuration files (nice to get all lines for logs)
yes | cp -r /opt/ci/ctaeos/etc /

eoshost=`hostname -f`

EOS_INSTANCE=`hostname -s`
TAPE_FS_ID=65535
CTA_BIN=/usr/bin/cta
CTA_KT=/etc/ctafrontend_SSS_c.keytab
CTA_XrdSecPROTOCOL=sss
CTA_PROC_DIR=/eos/${EOS_INSTANCE}/proc/cta
CTA_WF_DIR=${CTA_PROC_DIR}/workflow
CTA_TEST_DIR=/eos/${EOS_INSTANCE}/cta

# prepare CTA cli commands environment
cat <<EOF > /etc/cta/cta-cli.conf
# The CTA frontend address in the form <FQDN>:<TCPPort>
# solved by kubernetes DNS server so KIS...
ctafrontend:10955
EOF


# setup eos host and instance name
  sed -i -e "s/DUMMY_HOST_TO_REPLACE/${eoshost}/" /etc/sysconfig/eos
  sed -i -e "s/DUMMY_INSTANCE_TO_REPLACE/${EOS_INSTANCE}/" /etc/sysconfig/eos
  sed -i -e "s/DUMMY_HOST_TO_REPLACE/${eoshost}/" /etc/xrd.cf.mgm
  sed -i -e "s/DUMMY_INSTANCE_TO_REPLACE/${EOS_INSTANCE}/" /etc/xrd.cf.mgm
  sed -i -e "s/DUMMY_HOST_TO_REPLACE/${eoshost}/" /etc/xrd.cf.mq
  sed -i -e "s/DUMMY_HOST_TO_REPLACE/${eoshost}/" /etc/xrd.cf.fst

# prepare eos startup
  # skip systemd for eos initscripts
    export SYSTEMCTL_SKIP_REDIRECT=1
#  echo y | xrdsssadmin -k ${EOS_INSTANCE} -u daemon -g daemon add /etc/eos.keytab
# need a deterministic key for taped
# can only have one key????
echo -n '0 u:daemon g:daemon n:ctaeos+ N:6361884315374059521 c:1481241620 e:0 f:0 k:1a08f769e9c8e0c4c5a7e673247c8561cd23a0e7d8eee75e4a543f2d2dd3fd22' > /etc/eos.keytab 
    chmod 400 /etc/eos.keytab
    chown daemon:daemon /etc/eos.keytab
  mkdir -p /run/lock/subsys
  mkdir -p /var/eos/config/${eoshost}
    chown daemon:root /var/eos/config/${eoshost}
  touch   /var/eos/config/${eoshost}/default.eoscf
    chown daemon:daemon /var/eos/config/${eoshost}/default.eoscf

# add taped SSS must be in a kubernetes secret
#echo >> /etc/eos.keytab
#echo '0 u:stage g:tape n:taped+ N:6361736405290319874 c:1481207182 e:0 f:0 k:8e2335f24cf8c7d043b65b3b47758860cbad6691f5775ebd211b5807e1a6ec84' >> /etc/eos.keytab

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

  eos vid enable krb5
  eos vid enable sss
  eos vid enable unix
  EOS_MGM_URL="root://${eoshost}" eosfstregister -r /fst default:1

  # Add user daemon to sudoers this is to allow recalls for the moment using this command
  #  XrdSecPROTOCOL=sss xrdfs ctaeos prepare -s "/eos/ctaeos/cta/${TEST_FILE_NAME}?eos.ruid=12001&eos.rgid=1200"
  eos vid set membership 2 +sudo

  eos node set ${eoshost} on
  eos space set default on
  eos attr -r set default=replica /eos
  eos attr -r set sys.forced.nstripes=1 /eos

  eos fs add -m ${TAPE_FS_ID} tape localhost:1234 /does_not_exist tape
  eos mkdir ${CTA_PROC_DIR}
  eos mkdir ${CTA_WF_DIR}
  eos attr set CTA_TapeFsId=${TAPE_FS_ID} ${CTA_WF_DIR}
  
  # ${CTA_TEST_DIR} must be writable by eosusers and powerusers
  # but as there is no sticky bit in eos, we need to remove deletion for non owner to eosusers members
  # this is achieved through the ACLs.
  # ACLs in EOS are evaluated when unix permissions are failing, hence the 555 unix permission.
  eos mkdir ${CTA_TEST_DIR}
  eos chmod 555 ${CTA_TEST_DIR}
  eos attr set sys.acl=g:eosusers:rwx!d,g:powerusers:rwx+d /eos/ctaeos/cta

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
eos attr set sys.workflow.closew.default="bash:shell:cta XrdSecPROTOCOL=${CTA_XrdSecPROTOCOL} XrdSecSSSKT=${CTA_KT} ${CTA_BIN} archive --user <eos::wfe::rusername> --group <eos::wfe::rgroupname> --diskid <eos::wfe::fid> --instance eoscta --srcurl <eos::wfe::turl> --size <eos::wfe::size> --checksumtype <eos::wfe::checksumtype> --checksumvalue <eos::wfe::checksum> --storageclass <eos::wfe::cxattr:CTA_StorageClass> --diskfilepath <eos::wfe::path> --diskfileowner <eos::wfe::username> --diskfilegroup <eos::wfe::groupname> --recoveryblob:base64 <eos::wfe::base64:metadata> --reportURL 'eosQuery://${EOS_INSTANCE}//eos/wfe/passwd?mgm.pcmd=event\&mgm.fid=<eos::wfe::fxid>\&mgm.logid=cta\&mgm.event=archived\&mgm.workflow=default\&mgm.path=/eos/wfe/passwd\&mgm.ruid=0\&mgm.rgid=0' --stderr" ${CTA_WF_DIR}

# Set the worflow rule for creating tape file replicas in the EOS namespace.
eos attr set sys.workflow.archived.default="bash:shell:cta eos file tag <eos::wfe::path> +<eos::wfe::cxattr:CTA_TapeFsId>" ${CTA_WF_DIR}

# Set the worfklow rule for retrieving file from tape.
eos attr set sys.workflow.sync::prepare.default="bash:shell:cta XrdSecPROTOCOL=${CTA_XrdSecPROTOCOL} XrdSecSSSKT=${CTA_KT} ${CTA_BIN} retrieve --user <eos::wfe::rusername> --group <eos::wfe::rgroupname> --id <eos::wfe::fxattr:sys.archiveFileId> --dsturl '<eos::wfe::turl>\&eos.ruid=0\&eos.rgid=0\&eos.injection=1\&eos.workflow=CTA_retrieve' --diskfilepath <eos::wfe::path> --diskfileowner <eos::wfe::username> --diskfilegroup <eos::wfe::groupname> --recoveryblob:base64 <eos::wfe::base64:metadata> --stderr" ${CTA_WF_DIR}

# Set the workflow rule for the closew event of the CTA_retrieve workflow.
# Using the CTA_retrieve workflow will prevent the default workflow from
# receiving the closew event.  Triggering the default workflow in this way would
# haved causes the unwanted action of copying the disk file to tape again.
# The action of the CTA_retrieve workflow when triggered by the closew event is
# to set the CTA_retrieved_timestamp attribute.
eos attr set sys.workflow.closew.CTA_retrieve="bash:shell:cta eos attr set 'CTA_retrieved_timestamp=\"\`date\`\"' <eos::wfe::path>" ${CTA_WF_DIR}

echo "### ctaeos mgm ready ###"

/bin/bash
