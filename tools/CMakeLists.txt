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

find_package(Protobuf3 REQUIRED)

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

add_manpage(cta-admin)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-admin.1cta DESTINATION /usr/share/man/man1)
install(FILES cta-cli.example.conf DESTINATION usr/${CMAKE_INSTALL_DOCDIR})
