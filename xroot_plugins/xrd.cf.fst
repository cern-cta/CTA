# Load the plugin XrdProFst
xrootd.fslib /home/dkruse/CTA/build/xroot_plugins/libXrdProFst.so

# Use the Kerberos 5 security module
xrootd.seclib libXrdSec.so

# Using my own hardcoded keytab for the moment
sec.protocol krb5 /etc/krb5.keytab.dkruse host/<host>@CERN.CH

# Only Kerberos 5 is allowed
sec.protbind * only krb5

# Allow copying from absolute paths
all.export /

# Turn off asynchronous i/o
xrootd.async off

