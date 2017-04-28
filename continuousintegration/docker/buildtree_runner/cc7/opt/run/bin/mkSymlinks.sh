#!/bin/sh

# make symbolic links to all CTA binaries.

# Following binary list is build from (run in the build tree):
# find -type f -executable | egrep -v "\.so$" | egrep -v "\.sh$" | grep -v RPM/BUILD | grep -v CMake | grep -v CPack

echo Creating symlinks for CTA binaries and symlinks.
ln -s -v -t /usr/bin \
  ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/catalogue/cta-catalogue-admin-host-create \
  ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/catalogue/cta-database-poll \
  ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/catalogue/cta-catalogue-admin-user-create \
  ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/catalogue/cta-catalogue-schema-create \
  ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/catalogue/cta-catalogue-schema-drop \
  ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/cmdline/cta \
  ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/objectstore/dumpObject \
  ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/objectstore/listObjectStore \
  ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/objectstore/makeMinimalVFS \
  ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/objectstore/cta-objectstore-dump-object \
  ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/objectstore/cta-objectstore-list \
  ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/objectstore/cta-objectstore-initialize \
  ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/nameserver/makeMockNameServerBasePath \
  ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/tapeserver/castor/tape/tapeserver/drive/TapeDriveReadWriteTest \
  ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/tapeserver/castor/tape/tapeserver/daemon/cta-tapeserverd \
  ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/tapeserver/cta-taped \
  ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/tests/cta-systemTests \
  ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/tests/cta-catalogueUnitTests \
  ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/tests/cta-unitTests \
  ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/tests/cta-unitTests-multiProcess \
  ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/tests/cta-oraUnitTests \
  ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/xroot_plugins/cta-xrootd_plugins-fakeeos \
  ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/mediachanger/cta-mediachanger-dismount \
  ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/mediachanger/cta-mediachanger-mount \
  ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/disk/xrdclfsopaquetest \
  ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/xroot-client-test/xrootHammer
ln -s -v -t /usr/lib64 \
  ${BUILDTREE_BASE}/${BUILDTREE_SUBDIR}/xroot_plugins/libXrdCtaOfs.so
  
