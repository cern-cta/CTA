#!/bin/sh

#
## $Id: dlfmysetup.sh,v 1.5 2005/12/09 10:08:15 sponcec3 Exp $
#

killall -9 dlfserver

MYSQL=`which mysql 2>/dev/null`
if [ $? -ne 0 ]; then
  echo "Please setup MYSQL in your environment"
  exit 1
fi

if [ ! -s /etc/castor/DLFCONFIG ]; then
  echo "Please put login/passwd@DB in /etc/castor/DLFCONFIG"
  exit 1
fi

mysql `cat /etc/castor/DLFCONFIG` < dlf_mysql_drop.sql
mysql `cat /etc/castor/DLFCONFIG` < dlf_mysql_tbl.sql

/usr/bin/dlfserver

dlfenterfacility -F rtcpcld -n 0
dlfenterfacility -F migrator -n 1
dlfenterfacility -F recaller -n 2
dlfenterfacility -F stager -n 3
dlfenterfacility -F RHLog -n 4
dlfenterfacility -F job -n 5
dlfenterfacility -F MigHunter -n 6
dlfenterfacility -F rmmaster -n 7
dlfenterfacility -F GC -n 8
dlfenterfacility -F scheduler -n 9
dlfenterfacility -F TapeErrorHandler -n 10
dlfenterfacility -F Vdqm -n 11
dlfenterfacility -F rfio -n 12
dlfenterfacility -F SRMServer -n 13
dlfenterfacility -F SRMDaemon -n 14

dlflistfacility
