# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

cmake_minimum_required (VERSION 3.17)

add_subdirectory (cta-readtp)
add_subdirectory (cta-tape-label)
add_subdirectory (cta-release)
add_subdirectory (cta-statistics)
add_subdirectory (cta-catalogue-admin-user-create)
add_subdirectory (cta-catalogue-schema-create)
add_subdirectory (cta-catalogue-schema-drop)
add_subdirectory (cta-catalogue-schema-set-production)
add_subdirectory (cta-catalogue-schema-verify)
add_subdirectory (cta-database-poll)
add_subdirectory (standalone_cli_tools)

find_package(xrootdclient REQUIRED)
find_package(Protobuf3 REQUIRED)

# XRootD SSI
include_directories(${XROOTD_INCLUDE_DIR} ${XROOTD_INCLUDE_DIR}/private)

# XRootD SSI Protocol Buffer bindings
include_directories(${XRD_SSI_PB_DIR}/include ${XRD_SSI_PB_DIR}/eos_cta/include)

# Compiled protocol buffers
include_directories(${CMAKE_BINARY_DIR}/eos_cta ${PROTOBUF3_INCLUDE_DIRS})

# Catalogue command line unit tests
set (CATALOGUE_CMD_LINE_UNIT_TESTS_LIB_SRC_FILES
  cta-catalogue-admin-user-create/CreateAdminUserCmdLineArgs.cpp
  cta-catalogue-admin-user-create/CreateAdminUserCmdLineArgsTest.cpp
  cta-catalogue-schema-create/CreateSchemaCmdLineArgs.cpp
  cta-catalogue-schema-create/CreateSchemaCmdLineArgsTest.cpp
  cta-catalogue-schema-drop/DropSchemaCmdLineArgs.cpp
  cta-catalogue-schema-drop/DropSchemaCmdLineArgsTest.cpp
  cta-catalogue-schema-verify/VerifySchemaCmdLineArgs.cpp
  cta-catalogue-schema-verify/VerifySchemaCmdLineArgsTest.cpp
  cta-database-poll/PollDatabaseCmdLineArgs.cpp
  cta-database-poll/PollDatabaseCmdLineArgsTest.cpp)

add_library (ctacataloguecmdlineunittests SHARED
  ${CATALOGUE_CMD_LINE_UNIT_TESTS_LIB_SRC_FILES})
set_property(TARGET ctacataloguecmdlineunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctacataloguecmdlineunittests PROPERTY VERSION "${CTA_LIBVERSION}")

install (TARGETS ctacataloguecmdlineunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

#
# cta-admin <admin_command> is the SSI version of "cta <admin_command>"
#
add_executable(cta-admin CtaAdminXrdCmd.cpp CtaAdminParsedCmd.cpp CtaAdminCmdParser.cpp CtaAdminTextFormatter.cpp)
target_link_libraries(cta-admin XrdSsiLib XrdUtils ctacommon stdc++fs ctatapedaemoncommon)
set_property (TARGET cta-admin APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})

#
# cta-wfe-test archive|retrieve|delete <options> allows testing of the SSI WorkFlow Engine hooks
# without invoking EOS.
#
# Previously this was the eoscta_stub which was called by a script invoked by the EOS WFE.
#
add_executable(cta-wfe-test EosCtaStub.cpp)
target_link_libraries(cta-wfe-test ctacommon XrdSsiLib XrdUtils)
set_property (TARGET cta-wfe-test APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})

add_manpage(cta-admin)

install(TARGETS cta-admin DESTINATION usr/bin)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-admin.1cta DESTINATION /usr/share/man/man1)
install(PROGRAMS cta-send-closew.sh DESTINATION usr/bin)
install(FILES cta-cli.example.conf DESTINATION usr/${CMAKE_INSTALL_DOCDIR})
install(FILES cta-cli-grpc.example.conf DESTINATION usr/${CMAKE_INSTALL_DOCDIR})
