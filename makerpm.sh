#!/bin/sh

echo "### INFO ### Creating tarball"
# Extract package related information
specfile=`find . -maxdepth 1 -name '*.spec' -type f`
name=`awk '$1 == "Name:" { print $2 }' ${specfile}`
version=`awk '$1 == "Version:" { print $2 }' ${specfile}`
release=`awk '$1 == "Release:" { print $2 }' ${specfile}`

# Create the distribution tarball
rm -rf ${name}-${version}-${release}
rsync -aC --exclude '.__afs*' --exclude '.svn' --exclude '.git' --exclude '*build*' . ${name}-${version}-${release}
tar -zcf ${name}-${version}-${release}.tar.gz ${name}-${version}-${release}
rm -rf ${name}-${version}-${release}

thisstatus=$?
if [ $thisstatus != 0 ]; then
    exit $thisstatus
fi

echo "### INFO ### Creating rpms"
buildtree=`mktemp -d`
mkdir -p ${buildtree}/{RPMS/{i386,i586,i686,x86_64},SPECS,BUILD,SOURCES,SRPMS}

rpmbuild --define "_topdir ${buildtree}" \
         -ts -vvv ${name}-${version}-${release}.tar.gz
thisstatus=$?
if [ $thisstatus != 0 ]; then
    exit $thisstatus
fi

rpmbuild --define "_topdir ${buildtree}" \
         -ta -vvv ${name}-${version}-${release}.tar.gz
thisstatus=$?

find ${buildtree}/{,S}RPMS -type f -name "*.rpm" -exec mv -v {} ./ \;
rm -rf ${buildtree}
rm ${name}-${version}-${release}.tar.gz

exit $thisstatus
