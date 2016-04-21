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

add_executable(cta-unitTests
  unit_tests.cpp
  ${GMOCK_SRC})

target_link_libraries(cta-unitTests
  ctacatalogueunittests
  ctaexceptionunittests
  ctaiounittests
  ctalegacymsgunittests
  ctamessagesunittests
  ctaserverunittests
  ctatapereactorunittests
  ctatapeserverdaemonunittests
  ctatapeserverdriveunittests
  ctatapeserverfileunittests
  ctatapeserverscsiunittests
  ctatapeserverutilsunittests
  ctautilsunittests
  ctacommon
  ctacommonunittests
  ctaobjectstore
  ctaobjectstoreunittests
  ctanameserverunittests
  ctaremotensunittests
  ctaschedulerunittests
  ctaio
  ctadaemonunittests
  ${GMOCK_LIB}
  gtest
  pthread)

add_executable(cta-oraUnitTests
  OraUnitTestsCmdLineArgs.cpp
  oraUnitTests.cpp
  ${GMOCK_SRC})

target_link_libraries(cta-oraUnitTests
        ctacatalogueoraunittests
        ${GMOCK_LIB}
        gtest
        pthread)

add_executable(cta-unitTests-multiProcess
  unit_tests.cpp
  ${GMOCK_SRC})

target_link_libraries(cta-unitTests-multiProcess
  ctadaemonunittests-multiprocess
  ctacommon
  ${GMOCK_LIB}
  gtest
  pthread
  protobuf
  sqlite3)

add_library(unitTestHelper
  TempFile.cpp)

add_library(systemTestHelper
  Subprocess.cpp)

add_library(systemTestHelperTests SHARED
  SubprocessSystemTests.cpp)

target_link_libraries(systemTestHelperTests
  systemTestHelper
  ctacommon)

add_executable(cta-systemTests
  system_tests.cpp
  ${GMOCK_SRC})

target_link_libraries(cta-systemTests
  systemTestHelper
  systemTestHelperTests
  cta-tapedSystemTests
  gtest
  pthread)

install(TARGETS cta-oraUnitTests cta-unitTests cta-unitTests-multiProcess cta-systemTests DESTINATION usr/bin)

install(TARGETS systemTestHelperTests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

configure_file(cta-valgrindUnitTests.sh.in ${CMAKE_CURRENT_BINARY_DIR}/cta-valgrindUnitTests.sh)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-valgrindUnitTests.sh cta-unitPlusSystemTests.sh DESTINATION usr/bin)
install(FILES valgrind.suppr helgrind.suppr DESTINATION usr/share/cta-${CTA_VERSION}/unittest/)
