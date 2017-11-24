#!/bin/bash

# make symbolic links to all CTA binaries.

echo Creating symlinks for CTA binaries.
ln -s -v -t /usr/bin `find ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR} -type f -executable | egrep -v '\.so(\.|$)' | egrep -v '\.sh$' | grep -v RPM/BUILD | grep -v CMake | grep -v CPack`
echo Creating symlinks for CTA libraries.
find ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR} | grep '.so$' | xargs -itoto ln -s -v -t /usr/lib64 toto
echo Creating symlink for frontend configuration file.
ln -s -v -t /etc/cta `perl -e 'while (<>) { if (/cta_SOURCE_DIR:STATIC=(.*)/ ) { print $1."\n"; } }' < ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/CMakeCache.txt`/xroot_plugins/cta-frontend-xrootd.conf
