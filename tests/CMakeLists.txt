# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set (CTA_UNIT_TEST_SOURCES runners/UnitTestsMain.cpp)
if(CTA_USE_PGSCHED)
  set (CTA_UNIT_TEST_SOURCES ${CTA_UNIT_TEST_SOURCES}
    ${PROJECT_SOURCE_DIR}/scheduler/rdbms/TemporaryPostgresInstance.cpp)
endif()

# Link Google Test and Mock in the runners only.
# Static copies in multiple shared test libraries destroy the same global state more than once.
add_executable(cta-unit-tests ${CTA_UNIT_TEST_SOURCES})

set (CTA_UNIT_TEST_LIBS
  ctacataloguecmdlineunittests
  ctacommon
  ctacommonunittests
  ctaruntimeunittests
  ctaprotobufunittests
  ctaexceptionunittests
  ctamediachangerunittests
  ctamaintdunittests
  cta-grpc-server-unit-tests
  cta-service-resources-unit-tests
  cta-admin-api-unit-tests
  cta-workflow-api-unit-tests
  ctainmemorycatalogueunittests
  ctainmemoryconnunittests
  ctainmemorystmtunittests
  ctardbmsunittests
  ctardbmswrapperunittests
  ctaschedulerunittests
  ctatapedunittests
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

target_link_libraries(cta-unit-tests ${CTA_UNIT_TEST_LIBS} ${PROTOBUF3_LIBRARIES})

add_executable(cta-unit-tests-rdbms
  runners/rdbms/RdbmsTestContext.cpp
  runners/rdbms/RdbmsUnitTestsMain.cpp)

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
  ${PROJECT_SOURCE_DIR}/common/process/threading/SubProcessTests.cpp)

target_link_libraries(cta-unit-tests-multiprocess
  ctacommon
  ${PROTOBUF3_LIBRARIES}
  gtest
  gmock
  pthread
  sqlite3)

if(CTA_USE_PGSCHED)
  # The temporary PostgreSQL test environment uses the RDBMS and scheduler schema libraries.
  target_link_libraries(cta-unit-tests-multiprocess ctardbms ctaschedulerschema)
endif()

add_library(cta-unit-test-helper
  support/TempFile.cpp support/TempDirectory.cpp)
target_link_libraries(ctaruntimeunittests PRIVATE cta-unit-test-helper)
target_link_libraries(ctacommonunittests cta-unit-test-helper)
target_link_libraries(cta-grpc-server-unit-tests cta-unit-test-helper)

# Tests register through shared-library initializers, without direct symbol references.
# Keep these libraries when the distribution enables --as-needed by default.
foreach(test_runner cta-unit-tests cta-unit-tests-rdbms cta-unit-tests-multiprocess)
  target_link_options(${test_runner} PRIVATE "LINKER:--no-as-needed")
endforeach()

if(CTA_BUILD_TEST_PACKAGES)
  install(
    TARGETS
      cta-unit-tests-rdbms
      cta-unit-tests
      cta-unit-tests-multiprocess
    DESTINATION
      ${CMAKE_INSTALL_BINDIR})
endif()

# Define dynamic-analysis options before expanding the runner templates.
set(VALGRIND valgrind)
set(VALGRIND_OPTS --leak-check=full --demangle=yes --gen-suppressions=all --error-exitcode=1 --max-threads=1000)
set(VALGRIND_OPTS_W_SUPPR ${VALGRIND_OPTS} --suppressions=${CMAKE_CURRENT_BINARY_DIR}/valgrind.suppr)
string (REPLACE ";" " " VALGRIND_OPTS_STR "${VALGRIND_OPTS}")

configure_file(analysis/cta-unit-tests-valgrind.sh.in ${CMAKE_CURRENT_BINARY_DIR}/cta-unit-tests-valgrind.sh)
configure_file(analysis/parallelTestsMakefile.in ${CMAKE_CURRENT_BINARY_DIR}/parallelTestsMakefile @ONLY)
configure_file(analysis/parallelTestsMakefile.local.in ${CMAKE_CURRENT_BINARY_DIR}/parallelTestsMakefile.local @ONLY)

if(CTA_WITH_ORACLE)
  configure_file(analysis/cta-unit-tests-rdbms-oracle.sh.in ${CMAKE_CURRENT_BINARY_DIR}/cta-unit-tests-rdbms-oracle.sh @ONLY)
endif ()

if(CTA_BUILD_TEST_PACKAGES)
  install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/cta-unit-tests-valgrind.sh DESTINATION ${CMAKE_INSTALL_BINDIR})
  install(FILES analysis/valgrind.suppr ${CMAKE_CURRENT_BINARY_DIR}/parallelTestsMakefile DESTINATION ${CMAKE_INSTALL_DATADIR}/cta/tests)
endif()

if(CTA_WITH_ORACLE AND CTA_BUILD_TEST_PACKAGES)
  install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/cta-unit-tests-rdbms-oracle.sh DESTINATION ${CMAKE_INSTALL_BINDIR})
  install(FILES analysis/cta-unit-tests-rdbms-oracle.suppr DESTINATION ${CMAKE_INSTALL_DATADIR}/cta/tests)
endif()

# Test runner and dynamic-analysis targets.
configure_file(analysis/valgrind.suppr valgrind.suppr COPYONLY)

add_custom_target(cta-unit-tests-full
  $<TARGET_FILE:cta-unit-tests>
  COMMAND $<TARGET_FILE:cta-unit-tests-multiprocess>
  COMMAND ${VALGRIND} ${VALGRIND_OPTS_W_SUPPR} $<TARGET_FILE:cta-unit-tests>
  COMMAND ${VALGRIND} ${VALGRIND_OPTS_W_SUPPR} --child-silent-after-fork=yes $<TARGET_FILE:cta-unit-tests-multiprocess>

  DEPENDS cta-unit-tests cta-unit-tests-multiprocess ${CMAKE_CURRENT_BINARY_DIR}/valgrind.suppr
  COMMENT "Running unit tests with memory leak detection" VERBATIM)

add_custom_target(cta-unit-tests-valgrind
  echo ${VALGRIND} ${VALGRIND_OPTS_W_SUPPR} $<TARGET_FILE:cta-unit-tests>
  COMMAND ${VALGRIND} ${VALGRIND_OPTS_W_SUPPR} $<TARGET_FILE:cta-unit-tests>
  COMMAND echo ${VALGRIND} ${VALGRIND_OPTS_W_SUPPR} --child-silent-after-fork=yes $<TARGET_FILE:cta-unit-tests-multiprocess>
  COMMAND ${VALGRIND} ${VALGRIND_OPTS_W_SUPPR} --child-silent-after-fork=yes $<TARGET_FILE:cta-unit-tests-multiprocess>

  DEPENDS cta-unit-tests cta-unit-tests-multiprocess ${CMAKE_CURRENT_BINARY_DIR}/valgrind.suppr
  COMMENT "Running unit tests with memory leak detection" VERBATIM)

add_custom_target(cta-unit-tests-short
  $<TARGET_FILE:cta-unit-tests>
  COMMAND $<TARGET_FILE:cta-unit-tests-multiprocess>
  COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target cta-rust-tests

  DEPENDS cta-unit-tests cta-unit-tests-multiprocess cta-rust-tests
  COMMENT "Running C++ and Rust unit tests" VERBATIM)
