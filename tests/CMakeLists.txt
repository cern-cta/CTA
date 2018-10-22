# The CERN Tape Archive (CTA) project
# Copyright (C) 2015  CERN
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
cmake_minimum_required (VERSION 2.6)

find_package(gmock REQUIRED)
find_package(Protobuf3 REQUIRED)

include_directories(${PROTOBUF3_INCLUDE_DIRS})
add_executable(cta-unitTests
  unit_tests.cpp
  ${GMOCK_SRC})

target_link_libraries(cta-unitTests
  ctainmemorycatalogueunittests
  ctaexceptionunittests
  ctatapeserverdaemonunittests
  ctatapeserverdriveunittests
  ctatapeserverfileunittests
  ctatapeserverscsiunittests
  ctacataloguecmdlineunittests
  ctacommon
  ctacommonunittests
  ctaobjectstore
  ctaobjectstoreunittests
  ctardbmsunittests
  ctardbmswrapperunittests
  ctaschedulerunittests
  ctadaemonunittests
  ctamediachangerunittests
  ctamediachangeracsdaemonunittests
  ctamediachangeracsunittests
  ${GMOCK_LIB}
  gtest
  pthread)

set_property (TARGET cta-unitTests APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})
if (OCCI_SUPPORT)
  set_property (TARGET cta-unitTests APPEND PROPERTY INSTALL_RPATH ${ORACLE-INSTANTCLIENT_RPATH})
endif (OCCI_SUPPORT)

add_executable(cta-catalogueUnitTests
  CatalogueUnitTestsCmdLineArgs.cpp
  catalogueUnitTests.cpp
  ${GMOCK_SRC})

target_link_libraries(cta-catalogueUnitTests
  ctacatalogueunittests
  ${GMOCK_LIB}
  gtest
  pthread)

if (OCCI_SUPPORT)
  set_property (TARGET cta-catalogueUnitTests APPEND PROPERTY INSTALL_RPATH ${ORACLE-INSTANTCLIENT_RPATH})
endif (OCCI_SUPPORT)

add_executable(cta-unitTests-multiProcess
  unit_tests.cpp
  ${GMOCK_SRC})

target_link_libraries(cta-unitTests-multiProcess
  ctadaemonunittests-multiprocess
  ctacommon
  ${GMOCK_LIB}
  gtest
  pthread
  ${PROTOBUF3_LIBRAIRES}
  sqlite3)

set_property (TARGET cta-unitTests-multiProcess APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})
if (OCCI_SUPPORT)
  set_property (TARGET cta-unitTests-multiProcess APPEND PROPERTY INSTALL_RPATH ${ORACLE-INSTANTCLIENT_RPATH})
endif (OCCI_SUPPORT)

add_library(unitTestHelper
  TempFile.cpp)

add_library(systemTestHelperTests SHARED
  SubprocessSystemTests.cpp)
set_property(TARGET systemTestHelperTests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET systemTestHelperTests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries(systemTestHelperTests
  ctacommon)

add_executable(cta-systemTests
  system_tests.cpp
  ${GMOCK_SRC})

target_link_libraries(cta-systemTests
  systemTestHelperTests
  cta-tapedSystemTests
  gtest
  pthread)

install(TARGETS cta-catalogueUnitTests cta-unitTests cta-unitTests-multiProcess cta-systemTests DESTINATION usr/bin)

install(TARGETS systemTestHelperTests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

configure_file(cta-valgrindUnitTests.sh.in ${CMAKE_CURRENT_BINARY_DIR}/cta-valgrindUnitTests.sh)
configure_file(parallelTestsMakefile.in ${CMAKE_CURRENT_BINARY_DIR}/parallelTestsMakefile)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-valgrindUnitTests.sh cta-unitPlusSystemTests.sh DESTINATION usr/bin)
install(FILES valgrind.suppr helgrind.suppr ${CMAKE_CURRENT_BINARY_DIR}/parallelTestsMakefile DESTINATION usr/share/cta-${CTA_VERSION}/unittest/)
