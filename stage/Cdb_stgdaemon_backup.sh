#!/bin/sh
#
# $Id: Cdb_stgdaemon_backup.sh,v 1.1 2003/10/31 10:19:22 jdurand Exp $
#
# @(#)$RCSfile: Cdb_stgdaemon_backup.sh,v $ $Revision: 1.1 $ $Date: 2003/10/31 10:19:22 $ CERN/IT/ADC/CA Jean-Damien Durand
#

BCKHOME="/usr/spool/stage/bak"
export BCKHOME
mkdir -p $BCKHOME

BIN="/usr/local/bin"
export BIN

/bin/rm -f ${BCKHOME}/stgcat_castor.bak.tmp
/bin/rm -f ${BCKHOME}/stgpath_castor.bak.tmp
echo
echo "--- Calling stgconvert to dump DB into CASTOR disk-file format"
echo
${BIN}/stgconvert -q -Q -g -1 ${BCKHOME}/stgcat_castor.bak.tmp ${BCKHOME}/stgpath_castor.bak.tmp < /dev/null
if [ $? -eq 0 ]; then
    /bin/mv -f ${BCKHOME}/stgcat_castor.bak.tmp ${BCKHOME}/stgcat_castor.bak
    /bin/mv -f ${BCKHOME}/stgpath_castor.bak.tmp ${BCKHOME}/stgpath_castor.bak
    /bin/cp -f ${BCKHOME}/stgcat_castor.bak /tmp/stgcat_castor.bak
    /bin/cp -f ${BCKHOME}/stgpath_castor.bak /tmp/stgpath_castor.bak
else
  exit 1
fi

exit 0

