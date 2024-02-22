# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2022 CERN
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

add_subdirectory(common)

find_package(Protobuf3 REQUIRED)

include_directories(${PROJECT_BINARY_DIR}/tapeserver)

PROTOBUF3_GENERATE_CPP(WDMsgSources WDMsgHeaders WatchdogMessage.proto)

SET_SOURCE_FILES_PROPERTIES(${WDMsgHeaders} PROPERTIES HEADER_FILE_ONLY TRUE)
SET_SOURCE_FILES_PROPERTIES(DriveHandler.cpp PROPERTIES OBJECT_DEPENDS ${WDMsgHeaders})

include_directories(${PROTOBUF3_INCLUDE_DIRS})
add_library(ctatapedaemon
  ${WDMsgSources}
  CommandLineParams.cpp
  DriveHandler.cpp
  DriveHandlerProxy.cpp
  DriveHandlerStateReporter.cpp
  MaintenanceHandler.cpp
  ProcessManager.cpp
  SignalHandler.cpp
  TapeDaemon.cpp
  DriveConfigEntry.cpp
)

target_link_libraries(ctatapedaemon
  ctatapesession
  ctaTapeServerDaemon
  ctatapedaemoncommon
  SCSI
  TapeDrive
  File)

add_library(ctadaemonunittests SHARED
  DriveHandlerTests.cpp
)
set_property(TARGET ctadaemonunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctadaemonunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

add_library(ctadaemonunittests-multiprocess SHARED
  DriveHandlerTests.cpp
  ProcessManagerTests.cpp
  SignalHandlerTests.cpp
)
set_property(TARGET ctadaemonunittests-multiprocess PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctadaemonunittests-multiprocess PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries(ctadaemonunittests
  ctatapedaemon
  ctatapedaemoncommonunittests
  unitTestHelper)

target_link_libraries(ctadaemonunittests-multiprocess
  ctatapedaemon
  unitTestHelper)

install(TARGETS ctadaemonunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
install(TARGETS ctadaemonunittests-multiprocess DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

install (FILES cta-taped.conf.example
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)
