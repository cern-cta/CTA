# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2015-2021 CERN
# @license        This program is free software: you can redistribute it and/or modify
#                 it under the terms of the GNU General Public License as published by
#                 the Free Software Foundation, either version 3 of the License, or
#                 (at your option) any later version.
#
#                 This program is distributed in the hope that it will be useful,
#                 but WITHOUT ANY WARRANTY; without even the implied warranty of
#                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#                 GNU General Public License for more details.
#
#                 You should have received a copy of the GNU General Public License
#                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
cmake_minimum_required (VERSION 3.17)

find_package(gmock REQUIRED)
find_package(Protobuf3 REQUIRED)

include_directories(${PROTOBUF3_INCLUDE_DIRS})
add_executable(cta-unitTests
  unit_tests.cpp
  ${GMOCK_SRC})

target_link_libraries(cta-unitTests
  ctacataloguecmdlineunittests
  ctacommon
  ctacommonunittests
  ctadaemonunittests
  ctaexceptionunittests
  ctamediachangerunittests
  ctainmemorycatalogueunittests
  ctainmemoryconnunittests
  ctainmemorystmtunittests
  ctaobjectstore
  ctaobjectstoreunittests
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
  pthread
  ctatapeserverraounittests
)

set_property (TARGET cta-unitTests APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})
if (OCCI_SUPPORT)
  set_property (TARGET cta-unitTests APPEND PROPERTY INSTALL_RPATH ${ORACLE-INSTANTCLIENT_RPATH})
endif (OCCI_SUPPORT)

add_executable(cta-rdbmsUnitTests
  GlobalCatalogueFactoryForUnitTests.cpp
  RdbmsUnitTestsCmdLineArgs.cpp
  rdbmsUnitTests.cpp
  ${GMOCK_SRC})

target_link_libraries(cta-rdbmsUnitTests
  ctadbconfigcatalogueunittests
  ctadbconfigconnunittests
  ctadbconfigstmtunittests
  ctardbmsunittests
  ctadisk
  gtest
  pthread
  ${PROTOBUF3_LIBRARIES})

set_property (TARGET cta-rdbmsUnitTests APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})
if (OCCI_SUPPORT)
  set_property (TARGET cta-rdbmsUnitTests APPEND PROPERTY INSTALL_RPATH ${ORACLE-INSTANTCLIENT_RPATH})
endif (OCCI_SUPPORT)

add_executable(cta-unitTests-multiProcess
  unit_tests.cpp
  ${GMOCK_SRC})

target_link_libraries(cta-unitTests-multiProcess
  ctadaemonunittests-multiprocess
  ctacommon
  gtest
  pthread
  ${PROTOBUF3_LIBRAIRES}
  sqlite3)

set_property (TARGET cta-unitTests-multiProcess APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})
if (OCCI_SUPPORT)
  set_property (TARGET cta-unitTests-multiProcess APPEND PROPERTY INSTALL_RPATH ${ORACLE-INSTANTCLIENT_RPATH})
endif (OCCI_SUPPORT)

add_library(unitTestHelper
  TempFile.cpp TempDirectory.cpp)

add_library(systemTestHelperTests SHARED
  SubprocessSystemTests.cpp)
set_property(TARGET systemTestHelperTests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET systemTestHelperTests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries(systemTestHelperTests
  ctacommon)

add_executable(cta-systemTests
  system_tests.cpp
  ${GMOCK_SRC})

set_property (TARGET cta-systemTests APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})

target_link_libraries(cta-systemTests
  systemTestHelperTests
  cta-tapedSystemTests
  gtest
  pthread
  ${PROTOBUF3_LIBRARIES})

add_executable(cta-immutable-file-test
  ImmutableFileTest.cpp
  ImmutableFileTestMain.cpp
  ImmutableFileTestCmdLineArgs.cpp)

set_property (TARGET cta-immutable-file-test APPEND PROPERTY INCLUDE_DIRECTORIES "/usr/include/xrootd")
set_property (TARGET cta-immutable-file-test APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})

target_link_libraries(cta-immutable-file-test
  ctacommon)

install(
  TARGETS
    cta-rdbmsUnitTests
    cta-unitTests
    cta-unitTests-multiProcess
    cta-systemTests
    cta-immutable-file-test
  DESTINATION
    usr/bin)

install(TARGETS systemTestHelperTests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

configure_file(cta-rdbmsUnitTests-oracle.sh.in ${CMAKE_CURRENT_BINARY_DIR}/cta-rdbmsUnitTests-oracle.sh @ONLY)
configure_file(cta-valgrindUnitTests.sh.in ${CMAKE_CURRENT_BINARY_DIR}/cta-valgrindUnitTests.sh)
configure_file(parallelTestsMakefile.in ${CMAKE_CURRENT_BINARY_DIR}/parallelTestsMakefile @ONLY)
configure_file(parallelTestsMakefile.local.in ${CMAKE_CURRENT_BINARY_DIR}/parallelTestsMakefile.local @ONLY)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-rdbmsUnitTests-oracle.sh ${CMAKE_CURRENT_BINARY_DIR}/cta-valgrindUnitTests.sh cta-unitPlusSystemTests.sh DESTINATION usr/bin)
install(FILES cta-rdbmsUnitTests-oracle.suppr valgrind.suppr helgrind.suppr ${CMAKE_CURRENT_BINARY_DIR}/parallelTestsMakefile DESTINATION usr/share/cta-${CTA_VERSION}/unittest/)
