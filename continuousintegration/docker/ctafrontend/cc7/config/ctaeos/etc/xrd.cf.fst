###########################################################
set MGM=$EOS_MGM_ALIAS
###########################################################

xrootd.fslib -2 libXrdEosFst.so
xrootd.async off nosf
xrd.network keepalive
xrootd.redirect $(MGM):1094 chksum

###########################################################
xrootd.seclib libXrdSec.so
sec.protocol unix
sec.protocol sss -c /etc/eos.keytab -s /etc/eos.keytab
sec.protbind * only unix sss
###########################################################
all.export / nolock
all.trace none
#ofs.trace open
###########################################################
xrd.port 1095
ofs.persist off
ofs.osslib libEosFstOss.so
ofs.tpc pgm /bin/true
# ofs.tpc pgm should point to an existing executable file but it is completely ignored by eos ofs implementation
###########################################################
# this URL can be overwritten by EOS_BROKER_URL defined /etc/sysconfig/xrd
fstofs.broker root://DUMMY_HOST_TO_REPLACE:1097//eos/
fstofs.autoboot true
fstofs.quotainterval 10
fstofs.metalog /var/eos/md/
#fstofs.trace client
###########################################################

# Set the endpoint and resources for EOS/CTA Workflows
fstofs.protowfendpoint ctafrontend:10955
fstofs.protowfresource /ctafrontend
