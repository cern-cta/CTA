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

add_subdirectory(messages)

list (APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
find_package (xrootd REQUIRED)
include_directories(${CMAKE_SOURCE_DIR})
include_directories(${XROOTD_INCLUDE_DIR} ${XROOTD_PRIVATE_INCLUDE_DIR})
include_directories(${CMAKE_BINARY_DIR})
include_directories(${CMAKE_SOURCE_DIR}/tapeserver)

add_library (XrdCtaOfs MODULE XrdCtaFilesystem.cpp XrdCtaFile.cpp XrdCtaDir.cpp)
target_link_libraries (XrdCtaOfs ctacatalogue ctascheduler ctacommon protobuf ctaobjectstore cryptopp)

INSTALL (TARGETS XrdCtaOfs DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
INSTALL (FILES xrootd-cta.cfg DESTINATION /etc/xrootd/)
INSTALL (FILES cta-frontend.conf DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta)

set (FAKE_EOS_CMD_SRC_FILES
  CmdLineTool.cpp
  FakeEosCmd.cpp
  FakeEosCmdLineArgs.cpp
  FakeEosCmdMain.cpp)

add_executable (cta-xrootd_plugins-fakeeos ${FAKE_EOS_CMD_SRC_FILES})
target_link_libraries (cta-xrootd_plugins-fakeeos ctacommon)
