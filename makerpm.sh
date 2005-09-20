#!/bin/sh

# $Id: makerpm.sh,v 1.2 2005/09/20 12:05:04 jdurand Exp $

#
## Do tarball
#
. ./maketar.sh
thisstatus=$?
if [ $thisstatus != 0 ]; then
    exit $thisstatus
fi

#
## Build the package
#
cd ..
a=`echo ${MAJOR_CASTOR_VERSION} | sed 's/\..*//g'`
b=`echo ${MAJOR_CASTOR_VERSION} | sed 's/.*\.//g'`
c=`echo ${MINOR_CASTOR_VERSION} | sed 's/\..*//g'`
d=`echo ${MINOR_CASTOR_VERSION} | sed 's/.*\.//g'`

echo "### INFO ### Creating rpms"

rpmbuild -ta castor-${a}.${b}.${c}.tar.gz
thisstatus=$?

exit $thisstatus
