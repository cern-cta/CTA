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

list (APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
find_package (xrootd REQUIRED)
include_directories(${CMAKE_SOURCE_DIR})
include_directories(${XROOTD_INCLUDE_DIR} ${XROOTD_PRIVATE_INCLUDE_DIR})
include_directories(${CMAKE_BINARY_DIR})
include_directories(${CMAKE_SOURCE_DIR}/tapeserver)

find_package(Protobuf3 REQUIRED)
include_directories(${PROTOBUF3_INCLUDE_DIRS})
add_library (XrdCtaOfs MODULE ListArchiveFilesCmd.cpp XrdCtaFilesystem.cpp XrdCtaFile.cpp XrdCtaDir.cpp)
target_link_libraries (XrdCtaOfs ctacatalogue ctaeosmessages ctascheduler ctacommon ${PROTOBUF3_LIBRARIES} ctaobjectstore cryptopp)
set_target_properties(XrdCtaOfs PROPERTIES INSTALL_RPATH ${PROTOBUF3_RPATH})

INSTALL (TARGETS XrdCtaOfs DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
INSTALL (FILES xrootd-cta.cfg DESTINATION /etc/xrootd/)
INSTALL (FILES cta-frontend.conf DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta)
install (FILES cta-frontend.logrotate DESTINATION /etc/logrotate.d RENAME cta-frontend)

