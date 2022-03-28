# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2003-2022 CERN
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

include_directories(${CMAKE_SOURCE_DIR}/tapeserver)

set(TAPEDRIVE_LIBRARY_SRCS
  DriveGeneric.cpp
  FakeDrive.cpp)

add_library(TapeDrive ${TAPEDRIVE_LIBRARY_SRCS})
target_link_libraries(TapeDrive SCSI ctatapedaemon)

add_executable(TapeDriveReadWriteTest
  TapeDriveReadWriteTest.cpp)
target_link_libraries(TapeDriveReadWriteTest
  TapeDrive
  ctamediachanger
  SCSI
  System
  ctacommon
  #ctaclient
  ctaTapeServerDaemon
  gtest
  pthread)

add_library(ctatapeserverdriveunittests SHARED
  DriveTest.cpp)
set_property(TARGET ctatapeserverdriveunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctatapeserverdriveunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries(ctatapeserverdriveunittests
  ctaTapeServerDaemon
  System
  TapeDrive)

install(TARGETS ctatapeserverdriveunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
