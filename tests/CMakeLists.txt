# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2015-2024 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.
cmake_minimum_required (VERSION 3.17)

add_executable(cta-unitTests
  unit_tests.cpp)

set (CTA_UNIT_TEST_LIBS
  ctacataloguecmdlineunittests
  ctacommon
  ctacommonunittests
  ctafrontendcommonconfigunittests
  ctadaemonunittests
  ctaexceptionunittests
  ctamediachangerunittests
  ctainmemorycatalogueunittests
  ctainmemoryconnunittests
  ctainmemorystmtunittests
  ctardbmsunittests
  ctardbmswrapperunittests
  ctaschedulerunittests
  ctatapeserverdaemonunittests
  ctatapeserverdriveunittests
  ctatapeserverfileunittests
  ctatapeserverscsiunittests
  ctadiskunittests
  ctatapelabelunittests
  gtest
  gmock
  pthread
  ctatapeserverraounittests)

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
  unit_tests.cpp)

target_link_libraries(cta-unitTests-multiProcess
  ctadaemonunittests-multiprocess
  ctacommon
  gtest
  gmock
  pthread
  sqlite3)

add_library(unitTestHelper
  TempFile.cpp TempDirectory.cpp)

add_library(integrationTestHelperTests SHARED
  SubprocessIntegrationTests.cpp)
set_property(TARGET integrationTestHelperTests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET integrationTestHelperTests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries(integrationTestHelperTests
  ctacommon)

add_executable(cta-integrationTests
  integration_tests.cpp)

target_link_libraries(cta-integrationTests
  integrationTestHelperTests
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

install(TARGETS integrationTestHelperTests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

configure_file(cta-rdbmsUnitTests-oracle.sh.in ${CMAKE_CURRENT_BINARY_DIR}/cta-rdbmsUnitTests-oracle.sh @ONLY)
configure_file(cta-valgrindUnitTests.sh.in ${CMAKE_CURRENT_BINARY_DIR}/cta-valgrindUnitTests.sh)
configure_file(parallelTestsMakefile.in ${CMAKE_CURRENT_BINARY_DIR}/parallelTestsMakefile @ONLY)
configure_file(parallelTestsMakefile.local.in ${CMAKE_CURRENT_BINARY_DIR}/parallelTestsMakefile.local @ONLY)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-rdbmsUnitTests-oracle.sh ${CMAKE_CURRENT_BINARY_DIR}/cta-valgrindUnitTests.sh cta-unitPlusIntegrationTests.sh DESTINATION usr/bin)
install(FILES cta-rdbmsUnitTests-oracle.suppr valgrind.suppr helgrind.suppr ${CMAKE_CURRENT_BINARY_DIR}/parallelTestsMakefile DESTINATION usr/share/cta-${CTA_VERSION}/unittest/)
