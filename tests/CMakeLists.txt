# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set (CTA_UNIT_TEST_SOURCES unit_tests.cpp)
if(CTA_USE_PGSCHED)
  set (CTA_UNIT_TEST_SOURCES ${CTA_UNIT_TEST_SOURCES}
    ${PROJECT_SOURCE_DIR}/scheduler/rdbms/TemporaryPostgresInstance.cpp)
endif()

add_executable(cta-unit-tests ${CTA_UNIT_TEST_SOURCES})

set (CTA_UNIT_TEST_LIBS
  ctacataloguecmdlineunittests
  ctacommon
  ctacommonunittests
  ctaruntimeunittests
  ctaprotobufunittests
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

add_library(cta-unit-test-helper
  TempFile.cpp TempDirectory.cpp)

add_executable(cta-integration-tests
  integration_tests.cpp)

target_link_libraries(cta-integration-tests
  cta-taped-integration-tests
  gtest
  pthread)

add_executable(cta-test-immutable-file
  ImmutableFileTest.cpp
  ImmutableFileTestMain.cpp
  ImmutableFileTestCmdLineArgs.cpp)

set_property (TARGET cta-test-immutable-file APPEND PROPERTY INCLUDE_DIRECTORIES "/usr/include/xrootd")

target_link_libraries(cta-test-immutable-file
  ctacommon XrdCl)

install(
  TARGETS
    cta-unit-tests-rdbms
    cta-unit-tests
    cta-unit-tests-multiprocess
    cta-integration-tests
    cta-test-immutable-file
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

# Test runner and dynamic-analysis targets.
configure_file(valgrind.suppr valgrind.suppr COPYONLY)
configure_file(helgrind.suppr helgrind.suppr COPYONLY)
set(VALGRIND valgrind)
set(VALGRIND_OPTS --leak-check=full --demangle=yes --gen-suppressions=all --error-exitcode=1 --max-threads=1000)
set(VALGRIND_OPTS_W_SUPPR ${VALGRIND_OPTS} --suppressions=${CMAKE_CURRENT_BINARY_DIR}/valgrind.suppr)
string (REPLACE ";" " " VALGRIND_OPTS_STR "${VALGRIND_OPTS}")

set(HELGRIND_OPTS -v --demangle=yes --gen-suppressions=all --num-callers=25 --conflict-cache-size=10000000 --error-exitcode=1  --sim-hints=no-nptl-pthread-stackcache --max-threads=1000)
set(HELGRIND_OPTS_W_SUPPR ${HELGRIND_OPTS} --suppressions=${CMAKE_CURRENT_BINARY_DIR}/helgrind.suppr)
string (REPLACE ";" " " HELGRIND_OPTS_STR "${HELGRIND_OPTS}")


add_custom_target(cta-unit-tests-full
  $<TARGET_FILE:cta-unit-tests>
  COMMAND $<TARGET_FILE:cta-unit-tests-multiprocess>
  COMMAND ${VALGRIND} ${VALGRIND_OPTS_W_SUPPR} $<TARGET_FILE:cta-unit-tests>
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} $<TARGET_FILE:cta-unit-tests>
  COMMAND $<TARGET_FILE:cta-unit-tests-multiprocess>
  COMMAND ${VALGRIND} ${VALGRIND_OPTS_W_SUPPR} --child-silent-after-fork=yes $<TARGET_FILE:cta-unit-tests-multiprocess>
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} $<TARGET_FILE:cta-unit-tests-multiprocess>

  DEPENDS cta-unit-tests cta-unit-tests-multiprocess ${CMAKE_CURRENT_BINARY_DIR}/valgrind.suppr ${CMAKE_CURRENT_BINARY_DIR}/helgrind.suppr
  COMMENT "Running unit tests with memory leak and race conditions detection" VERBATIM)

add_custom_target(cta-unit-tests-valgrind
  echo ${VALGRIND} ${VALGRIND_OPTS_W_SUPPR} $<TARGET_FILE:cta-unit-tests>
  COMMAND ${VALGRIND} ${VALGRIND_OPTS_W_SUPPR} $<TARGET_FILE:cta-unit-tests>
  COMMAND echo ${VALGRIND} ${VALGRIND_OPTS_W_SUPPR} --child-silent-after-fork=yes $<TARGET_FILE:cta-unit-tests-multiprocess>
  COMMAND ${VALGRIND} ${VALGRIND_OPTS_W_SUPPR} --child-silent-after-fork=yes $<TARGET_FILE:cta-unit-tests-multiprocess>

  DEPENDS cta-unit-tests cta-unit-tests-multiprocess ${CMAKE_CURRENT_BINARY_DIR}/valgrind.suppr
  COMMENT "Running unit tests with memory leak detection" VERBATIM)

add_custom_target(cta-unit-tests-helgrind
  echo ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} $<TARGET_FILE:cta-unit-tests>
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} $<TARGET_FILE:cta-unit-tests>
  COMMAND echo ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} $<TARGET_FILE:cta-unit-tests-multiprocess>
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} $<TARGET_FILE:cta-unit-tests-multiprocess>

  DEPENDS cta-unit-tests cta-unit-tests-multiprocess ${CMAKE_CURRENT_BINARY_DIR}/helgrind.suppr
  COMMENT "Running unit tests with race conditions detection" VERBATIM)

add_custom_target(cta-unit-tests-short
  $<TARGET_FILE:cta-unit-tests>
  COMMAND $<TARGET_FILE:cta-unit-tests-multiprocess>

  DEPENDS cta-unit-tests cta-unit-tests-multiprocess
  COMMENT "Running unit tests" VERBATIM)

add_custom_target(cta-unit-tests-helgrind-base
  echo ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q $<TARGET_FILE:cta-unit-tests> --gtest_filter=-OStoreDBPlusMockSchedulerTestVFS*:OStoreTestVFS*:OStoreDBPlusMockSchedulerTestVFS*:InMemory*
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q $<TARGET_FILE:cta-unit-tests> --gtest_filter=-OStoreDBPlusMockSchedulerTestVFS*:OStoreTestVFS*:OStoreDBPlusMockSchedulerTestVFS*:InMemory*
  COMMAND echo ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q $<TARGET_FILE:cta-unit-tests-multiprocess>
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q $<TARGET_FILE:cta-unit-tests-multiprocess>

  DEPENDS cta-unit-tests cta-unit-tests-multiprocess ${CMAKE_CURRENT_BINARY_DIR}/helgrind.suppr
  COMMENT "Running unit tests with race conditions detection (except slow sections)" VERBATIM)

add_custom_target(cta-unit-tests-helgrind-scheduler
  echo ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q $<TARGET_FILE:cta-unit-tests> --gtest_filter=OStoreDBPlusMockSchedulerTestVFS/Scheduler*
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q $<TARGET_FILE:cta-unit-tests> --gtest_filter=OStoreDBPlusMockSchedulerTestVFS/Scheduler*

  DEPENDS cta-unit-tests cta-unit-tests-multiprocess ${CMAKE_CURRENT_BINARY_DIR}/helgrind.suppr
  COMMENT "Running unit tests with race conditions detection (Mock scheduler section)" VERBATIM)

add_custom_target(cta-unit-tests-helgrind-object-store-db
  echo ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q $<TARGET_FILE:cta-unit-tests> --gtest_filter=OStoreTestVFS*
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q $<TARGET_FILE:cta-unit-tests> --gtest_filter=OStoreTestVFS*

  DEPENDS cta-unit-tests cta-unit-tests-multiprocess ${CMAKE_CURRENT_BINARY_DIR}/helgrind.suppr
  COMMENT "Running unit tests with race conditions detection (Object store DB section)" VERBATIM)

add_custom_target(cta-unit-tests-helgrind-data-transfer
  echo ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q $<TARGET_FILE:cta-unit-tests> --gtest_filter=OStoreDBPlusMockSchedulerTestVFS/DataTransferSessionTest*
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q $<TARGET_FILE:cta-unit-tests> --gtest_filter=OStoreDBPlusMockSchedulerTestVFS/DataTransferSessionTest*

  DEPENDS cta-unit-tests cta-unit-tests-multiprocess ${CMAKE_CURRENT_BINARY_DIR}/helgrind.suppr
  COMMENT "Running unit tests with race conditions detection (Data transfer section)" VERBATIM)

add_custom_target(cta-unit-tests-helgrind-in-memory-catalogue
  echo ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q $<TARGET_FILE:cta-unit-tests> --gtest_filter=InMemory/*
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q $<TARGET_FILE:cta-unit-tests> --gtest_filter=InMemory/*

  DEPENDS cta-unit-tests cta-unit-tests-multiprocess ${CMAKE_CURRENT_BINARY_DIR}/helgrind.suppr
  COMMENT "Running unit tests with race conditions detection (In memory catalogue section)" VERBATIM)

add_custom_target(cta-unit-tests-helgrind-parallel
  echo

  DEPENDS cta-unit-tests-helgrind-base cta-unit-tests-helgrind-scheduler cta-unit-tests-helgrind-object-store-db cta-unit-tests-helgrind-data-transfer cta-unit-tests-helgrind-in-memory-catalogue)
