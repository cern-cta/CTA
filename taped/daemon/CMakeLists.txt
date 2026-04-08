# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

add_subdirectory(common)

find_package(Protobuf3 REQUIRED)

# So that we can see WatchdogMessage.pb.h
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# See https://gitlab.cern.ch/cta/CTA/-/issues/791#note_8251696
PROTOBUF3_GENERATE_CPP(WDMsgSources WDMsgHeaders WatchdogMessage.proto)

SET_SOURCE_FILES_PROPERTIES(${WDMsgHeaders} PROPERTIES HEADER_FILE_ONLY TRUE)
SET_SOURCE_FILES_PROPERTIES(DriveHandler.cpp PROPERTIES OBJECT_DEPENDS ${WDMsgHeaders})

add_library(ctatapedaemon
  ${WDMsgSources}
  DriveHandler.cpp
  DriveHandlerProxy.cpp
  DriveHandlerStateReporter.cpp
  ProcessManager.cpp
  SignalHandler.cpp
  TapeDaemon.cpp
  DriveConfigEntry.cpp
)

target_link_libraries(ctatapedaemon
  ctatapedsession
  ctatapedaemoncommon
  ctatapedscsi
  ctatapeddrive
  ctatapedfile)

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

install (FILES cta-taped.example.conf
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)
