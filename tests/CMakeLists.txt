# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set (CTA_UNIT_TEST_SOURCES unit_tests.cpp)
if(CTA_USE_PGSCHED)
  set (CTA_UNIT_TEST_SOURCES ${CTA_UNIT_TEST_SOURCES}
    ${PROJECT_SOURCE_DIR}/scheduler/rdbms/TemporaryPostgresInstance.cpp)
endif()

add_executable(cta-unitTests ${CTA_UNIT_TEST_SOURCES})

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

target_link_libraries(cta-unitTests ${CTA_UNIT_TEST_LIBS})

add_executable(cta-rdbmsUnitTests
  GlobalCatalogueFactoryForUnitTests.cpp
  RdbmsUnitTestsCmdLineArgs.cpp
  rdbmsUnitTests.cpp)

target_link_libraries(cta-rdbmsUnitTests
  ctadropschemacmd
  ctadbconfigcatalogueunittests
  ctadbconfigconnunittests
  ctadbconfigstmtunittests
  ctardbmsunittests
  ctadisk
  gtest
  gmock
  pthread)

add_executable(cta-unitTests-multiProcess
  ${CTA_UNIT_TEST_SOURCES}
  SubprocessTests.cpp)

target_link_libraries(cta-unitTests-multiProcess
  ctadaemonunittests-multiprocess
  ctacommon
  gtest
  gmock
  pthread
  sqlite3)

add_library(unitTestHelper
  TempFile.cpp TempDirectory.cpp)

add_executable(cta-integrationTests
  integration_tests.cpp)

target_link_libraries(cta-integrationTests
  cta-tapedIntegrationTests
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
    cta-rdbmsUnitTests
    cta-unitTests
    cta-unitTests-multiProcess
    cta-integrationTests
    cta-immutable-file-test
  DESTINATION
    usr/bin)

configure_file(cta-valgrindUnitTests.sh.in ${CMAKE_CURRENT_BINARY_DIR}/cta-valgrindUnitTests.sh)
configure_file(parallelTestsMakefile.in ${CMAKE_CURRENT_BINARY_DIR}/parallelTestsMakefile @ONLY)
configure_file(parallelTestsMakefile.local.in ${CMAKE_CURRENT_BINARY_DIR}/parallelTestsMakefile.local @ONLY)

if (OCCI_SUPPORT)
  configure_file(cta-rdbmsUnitTests-oracle.sh.in ${CMAKE_CURRENT_BINARY_DIR}/cta-rdbmsUnitTests-oracle.sh @ONLY)
endif ()

install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/cta-valgrindUnitTests.sh cta-unitPlusIntegrationTests.sh DESTINATION usr/bin)
install(FILES valgrind.suppr helgrind.suppr ${CMAKE_CURRENT_BINARY_DIR}/parallelTestsMakefile DESTINATION usr/share/cta-${CTA_VERSION}/unittest/)

if (OCCI_SUPPORT)
  install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/cta-rdbmsUnitTests-oracle.sh DESTINATION usr/bin)
  install(FILES cta-rdbmsUnitTests-oracle.suppr DESTINATION usr/share/cta-${CTA_VERSION}/unittest/)
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


add_custom_target(fullunittests
  $<TARGET_FILE:cta-unitTests>
  COMMAND $<TARGET_FILE:cta-unitTests-multiProcess>
  COMMAND ${VALGRIND} ${VALGRIND_OPTS_W_SUPPR} $<TARGET_FILE:cta-unitTests>
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} $<TARGET_FILE:cta-unitTests>
  COMMAND $<TARGET_FILE:cta-unitTests-multiProcess>
  COMMAND ${VALGRIND} ${VALGRIND_OPTS_W_SUPPR} --child-silent-after-fork=yes $<TARGET_FILE:cta-unitTests-multiProcess>
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} $<TARGET_FILE:cta-unitTests-multiProcess>

  DEPENDS cta-unitTests cta-unitTests-multiProcess ${CMAKE_CURRENT_BINARY_DIR}/valgrind.suppr ${CMAKE_CURRENT_BINARY_DIR}/helgrind.suppr
  COMMENT "Running unit tests with memory leak and race conditions detection" VERBATIM)

add_custom_target(valgrind
  echo ${VALGRIND} ${VALGRIND_OPTS_W_SUPPR} $<TARGET_FILE:cta-unitTests>
  COMMAND ${VALGRIND} ${VALGRIND_OPTS_W_SUPPR} $<TARGET_FILE:cta-unitTests>
  COMMAND echo ${VALGRIND} ${VALGRIND_OPTS_W_SUPPR} --child-silent-after-fork=yes $<TARGET_FILE:cta-unitTests-multiProcess>
  COMMAND ${VALGRIND} ${VALGRIND_OPTS_W_SUPPR} --child-silent-after-fork=yes $<TARGET_FILE:cta-unitTests-multiProcess>

  DEPENDS cta-unitTests cta-unitTests-multiProcess ${CMAKE_CURRENT_BINARY_DIR}/valgrind.suppr
  COMMENT "Running unit tests with memory leak detection" VERBATIM)

add_custom_target(helgrind
  echo ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} $<TARGET_FILE:cta-unitTests>
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} $<TARGET_FILE:cta-unitTests>
  COMMAND echo ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} $<TARGET_FILE:cta-unitTests-multiProcess>
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} $<TARGET_FILE:cta-unitTests-multiProcess>

  DEPENDS cta-unitTests cta-unitTests-multiProcess ${CMAKE_CURRENT_BINARY_DIR}/helgrind.suppr
  COMMENT "Running unit tests with race conditions detection" VERBATIM)

add_custom_target(shortunittests
  $<TARGET_FILE:cta-unitTests>
  COMMAND $<TARGET_FILE:cta-unitTests-multiProcess>

  DEPENDS cta-unitTests cta-unitTests-multiProcess
  COMMENT "Running unit tests" VERBATIM)

add_custom_target(helgrindBase
  echo ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q $<TARGET_FILE:cta-unitTests> --gtest_filter=-OStoreDBPlusMockSchedulerTestVFS*:OStoreTestVFS*:OStoreDBPlusMockSchedulerTestVFS*:InMemory*
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q $<TARGET_FILE:cta-unitTests> --gtest_filter=-OStoreDBPlusMockSchedulerTestVFS*:OStoreTestVFS*:OStoreDBPlusMockSchedulerTestVFS*:InMemory*
  COMMAND echo ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q $<TARGET_FILE:cta-unitTests-multiProcess>
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q $<TARGET_FILE:cta-unitTests-multiProcess>

  DEPENDS cta-unitTests cta-unitTests-multiProcess ${CMAKE_CURRENT_BINARY_DIR}/helgrind.suppr
  COMMENT "Running unit tests with race conditions detection (except slow sections)" VERBATIM)

add_custom_target(helgrindScheduler
  echo ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q $<TARGET_FILE:cta-unitTests> --gtest_filter=OStoreDBPlusMockSchedulerTestVFS/Scheduler*
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q $<TARGET_FILE:cta-unitTests> --gtest_filter=OStoreDBPlusMockSchedulerTestVFS/Scheduler*

  DEPENDS cta-unitTests cta-unitTests-multiProcess ${CMAKE_CURRENT_BINARY_DIR}/helgrind.suppr
  COMMENT "Running unit tests with race conditions detection (Mock scheduler section)" VERBATIM)

add_custom_target(helgrindOStoreDB
  echo ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q $<TARGET_FILE:cta-unitTests> --gtest_filter=OStoreTestVFS*
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q $<TARGET_FILE:cta-unitTests> --gtest_filter=OStoreTestVFS*

  DEPENDS cta-unitTests cta-unitTests-multiProcess ${CMAKE_CURRENT_BINARY_DIR}/helgrind.suppr
  COMMENT "Running unit tests with race conditions detection (Object store DB section)" VERBATIM)

add_custom_target(helgrindDataTransfer
  echo ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q $<TARGET_FILE:cta-unitTests> --gtest_filter=OStoreDBPlusMockSchedulerTestVFS/DataTransferSessionTest*
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q $<TARGET_FILE:cta-unitTests> --gtest_filter=OStoreDBPlusMockSchedulerTestVFS/DataTransferSessionTest*

  DEPENDS cta-unitTests cta-unitTests-multiProcess ${CMAKE_CURRENT_BINARY_DIR}/helgrind.suppr
  COMMENT "Running unit tests with race conditions detection (Data transfer section)" VERBATIM)

add_custom_target(helgrindInMemoryCatalogue
  echo ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q $<TARGET_FILE:cta-unitTests> --gtest_filter=InMemory/*
  COMMAND ${VALGRIND} --tool=helgrind ${HELGRIND_OPTS_W_SUPPR} -q $<TARGET_FILE:cta-unitTests> --gtest_filter=InMemory/*

  DEPENDS cta-unitTests cta-unitTests-multiProcess ${CMAKE_CURRENT_BINARY_DIR}/helgrind.suppr
  COMMENT "Running unit tests with race conditions detection (In memory catalogue section)" VERBATIM)

add_custom_target(parallelHelgrind
  echo

  DEPENDS helgrindBase helgrindScheduler helgrindOStoreDB helgrindDataTransfer helgrindInMemoryCatalogue)
