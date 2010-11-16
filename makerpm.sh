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
a=`echo ${MAJOR_CASTOR_VERSION} | sed 's/\..*//g'`
b=`echo ${MAJOR_CASTOR_VERSION} | sed 's/.*\.//g'`
c=`echo ${MINOR_CASTOR_VERSION} | sed 's/\..*//g'`
d=`echo ${MINOR_CASTOR_VERSION} | sed 's/.*\.//g'`

echo "### INFO ### Creating rpms"

buildtree=`mktemp -d`
mkdir -p ${buildtree}/{RPMS/{i386,i586,i686,x86_64},SPECS,BUILD,SOURCES,SRPMS}

t=`date +%Y-%m-%d-%H:%M`
rpmbuild --define "_topdir ${buildtree}" \
         -ta -vvv castor-${a}.${b}.${c}-${d}.tar.gz   \
         --pipe "tee ./castor-rpmbuild-log-${t}.txt"	
thisstatus=$?

mkdir -p RPMS
find ${buildtree} -type f -name "*.rpm" -exec mv {} RPMS/ \;
rm -rf ${buildtree}

exit $thisstatus
