#!/bin/sh

#
## $Id: dlfmysetup.sh,v 1.1 2005/05/19 12:06:44 jdurand Exp $
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

dlflistfacility
