# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

set (CTA_UNIT_TEST_SOURCES unit_tests.cpp)
if(CTA_USE_PGSCHED)
  set (CTA_UNIT_TEST_SOURCES ${CTA_UNIT_TEST_SOURCES}
    ${PROJECT_SOURCE_DIR}/scheduler/rdbms/TemporaryPostgresInstance.cpp)
endif()

add_executable(cta-unit-tests ${CTA_UNIT_TEST_SOURCES})

# link with TemporaryPostgresInstance if the pg scheduler is requested
if(CTA_USE_PGSCHED)
  set (CTA_UNIT_TEST_LIBS ${CTA_UNIT_TEST_LIBS}
    ctaschedulerschema
    ctaobjectstore
    ctaobjectstoreunittests)
endif()

set (CTA_UNIT_TEST_LIBS
  ctacataloguecmdlineunittests
  ctacommon
  ctacommonunittests
  ctafrontendcommonconfigunittests
  ctadaemonunittests
  ctaexceptionunittests
  ctamediachangerunittests
  ctamaintdunittests
  ctainmemorycatalogueunittests
  ctainmemoryconnunittests
  ctainmemorystmtunittests
  ctardbmsunittests
  ctardbmswrapperunittests
  ctaschedulerunittests
  ctatapedsessionunittests
  ctatapeddriveunittests
  ctatapedfileunittests
  ctascsiunittests
  ctadiskunittests
  ctatapelabelunittests
  gtest
  gmock
  pthread
  ctatapedraounittests)

if(NOT CTA_USE_PGSCHED)
  set (CTA_UNIT_TEST_LIBS ${CTA_UNIT_TEST_LIBS}
    ctaobjectstore
    ctaobjectstoreunittests)
endif()

target_link_libraries(cta-unit-tests ${CTA_UNIT_TEST_LIBS})

add_executable(cta-unit-tests-rdbms
  GlobalCatalogueFactoryForUnitTests.cpp
  RdbmsUnitTestsCmdLineArgs.cpp
  rdbmsUnitTests.cpp)

target_link_libraries(cta-unit-tests-rdbms
  ctadropschemacmd
  ctadbconfigcatalogueunittests
  ctadbconfigconnunittests
  ctadbconfigstmtunittests
  ctardbmsunittests
  ctadisk
  gtest
  gmock
  pthread)

add_executable(cta-unit-tests-multiprocess
  ${CTA_UNIT_TEST_SOURCES}
  SubprocessTests.cpp)

target_link_libraries(cta-unit-tests-multiprocess
  ctadaemonunittests-multiprocess
  ctacommon
  gtest
  gmock
  pthread
  sqlite3)

add_library(unit-test-helper
  TempFile.cpp TempDirectory.cpp)

add_executable(cta-integration-tests
  integration_tests.cpp)

target_link_libraries(cta-integration-tests
  cta-taped-integration-tests
  gtest
  pthread)

add_executable(cta-immutable-file-test
  ImmutableFileTest.cpp
  ImmutableFileTestMain.cpp
  ImmutableFileTestCmdLineArgs.cpp)

set_property (TARGET cta-immutable-file-test APPEND PROPERTY INCLUDE_DIRECTORIES "/usr/include/xrootd")

target_link_libraries(cta-immutable-file-test
  ctacommon XrdCl)

install(
  TARGETS
    cta-unit-tests-rdbms
    cta-unit-tests
    cta-unit-tests-multiprocess
    cta-integration-tests
    cta-immutable-file-test
  DESTINATION
    usr/bin)

configure_file(cta-unit-tests-valgrind.sh.in ${CMAKE_CURRENT_BINARY_DIR}/cta-unit-tests-valgrind.sh)
configure_file(parallelTestsMakefile.in ${CMAKE_CURRENT_BINARY_DIR}/parallelTestsMakefile @ONLY)
configure_file(parallelTestsMakefile.local.in ${CMAKE_CURRENT_BINARY_DIR}/parallelTestsMakefile.local @ONLY)

if (OCCI_SUPPORT)
  configure_file(cta-unit-tests-rdbms-oracle.sh.in ${CMAKE_CURRENT_BINARY_DIR}/cta-unit-tests-rdbms-oracle.sh @ONLY)
endif ()

install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/cta-unit-tests-valgrind.sh cta-unit-and-integration-tests.sh DESTINATION usr/bin)
install(FILES valgrind.suppr helgrind.suppr ${CMAKE_CURRENT_BINARY_DIR}/parallelTestsMakefile DESTINATION usr/share/cta-${CTA_VERSION}/unittest/)

if (OCCI_SUPPORT)
  install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/cta-unit-tests-rdbms-oracle.sh DESTINATION usr/bin)
  install(FILES cta-unit-tests-rdbms-oracle.suppr DESTINATION usr/share/cta-${CTA_VERSION}/unittest/)
endif ()
