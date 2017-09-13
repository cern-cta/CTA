#!/bin/bash

# make symbolic links to all CTA binaries.

echo Creating symlinks for CTA binaries and symlinks.
ln -s -v -t /usr/bin `find ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR} -type f -executable | egrep -v '\.so(\.|$)' | egrep -v '\.sh$' | grep -v RPM/BUILD | grep -v CMake | grep -v CPack`
find ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR} | grep '.so$' | xargs -itoto ln -s -v -t /usr/lib64 toto
