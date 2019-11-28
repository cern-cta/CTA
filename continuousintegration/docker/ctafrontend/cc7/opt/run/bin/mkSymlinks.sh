#!/bin/bash

# make symbolic links to all CTA binaries.



echo "Creating symlinks for CTA binaries."
ln -s -v -t /usr/bin `find ${BUILDTREE_BASE}/${CTA_BUILDTREE_SUBDIR} -type f -executable | egrep -v '\.so(\.|$)' | egrep -v '\.sh$' | grep -v RPM/BUILD | grep -v CMake | grep -v CPack`
echo Creating symlinks for CTA libraries.
find ${BUILDTREE_BASE}/${CTA_BUILDTREE_SUBDIR} | grep '.so$' | xargs -itoto ln -s -v -t /usr/lib64 toto
echo Creating symlink for frontend configuration file.
CTA_SOURCE_TREE=`perl -e 'while (<>) { if (/cta_SOURCE_DIR:STATIC=(.*)/ ) { print $1."\n"; } }' < ${BUILDTREE_BASE}/${CTA_BUILDTREE_SUBDIR}/CMakeCache.txt`
ln -s -v -t /etc/cta ${CTA_SOURCE_TREE}/xroot_plugins/cta-frontend-xrootd.conf
echo "Copying cta-fst-gcd (requires a different name)"
cp -v ${CTA_SOURCE_TREE}/python/eosfstgcd/ctafstgcd.py /usr/bin/cta-fst-gcd
if [[ -n "${EOS_BUILDTREE_SUBDIR}" ]]; then
  echo Creating symlinks for EOS binaries.
  ln -s -v -t /usr/bin `find ${BUILDTREE_BASE}/${EOS_BUILDTREE_SUBDIR} -type f -executable | egrep -v '\.so(\.|$)' | egrep -v '\.sh$' | grep -v RPM/BUILD | grep -v CMake | grep -v CPack`
  echo Creating symlinks for EOS libraries.
  find ${BUILDTREE_BASE}/${EOS_BUILDTREE_SUBDIR} | egrep 'lib.*\.so($|\.)' | xargs -itoto ln -s -v -t /usr/lib64 toto
  find ${BUILDTREE_BASE}/${EOS_BUILDTREE_SUBDIR} | egrep 'lib.*\.so($|\.)' | xargs -itoto ln -s -v -t /usr/lib64 toto.0
  ln -s -v -t /usr/sbin `grep eos_SOURCE ${BUILDTREE_BASE}/${EOS_BUILDTREE_SUBDIR}/CMakeCache.txt | cut -d = -f 2-`/fst/tools/eos*
  mkdir /var/log/eos{,/tx}
  mkdir /var/eos{,/wfe{,/bash},ns-queue{,/default}}
  chown -R daemon.daemon /var/log/eos /var/eos
fi
echo "Symlinks creation compete."
