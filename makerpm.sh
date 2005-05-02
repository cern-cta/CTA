#!/bin/sh

# $Id: makerpm.sh,v 1.1 2005/05/02 08:08:27 jdurand Exp $

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

#
## Great - the install is doing chown - which is allowed
## only as root
## Is there any fakeroot by change in here?
#
fakeroot=`which fakeroot 2>/dev/null`
if [ `id -u` -ne 0 ]; then
    if [ -z "${fakeroot}" ]; then
	echo "### WARNING ### The build is likely to fail if you are not root"
	sleep 1
	rpmbuild -ta castor-${a}.${b}.${c}.tar.gz
    else
	echo "### INFO ### Using fakeroot rpmbuild ..."
	fakeroot rpmbuild -ta castor-${a}.${b}.${c}.tar.gz
    fi
else
  rpmbuild -ta castor-${a}.${b}.${c}.tar.gz
fi
thisstatus=$?

exit $thisstatus
