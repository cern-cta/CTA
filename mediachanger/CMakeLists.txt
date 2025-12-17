# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

add_subdirectory(librmc)
add_subdirectory(rmcd)
add_subdirectory(smc)

set (MEDIACHANGER_LIB_SRC_FILES
  CmdLine.cpp
  CmdLineTool.cpp
  CommonMarshal.cpp
  DebugBuf.cpp
  io.cpp
  IpAndPort.cpp
  LibrarySlot.cpp
  LibrarySlotParser.cpp
  DummyLibrarySlot.cpp
  MediaChangerFacade.cpp
  MediaChangerProxy.cpp
  MessageHeader.cpp
  DmcProxy.cpp
  RmcMarshal.cpp
  RmcMarshal.hpp
  RmcMountMsgBody.cpp
  RmcProxy.cpp
  RmcUnmountMsgBody.cpp
  ScsiLibrarySlot.cpp
  TapeLibraryType.cpp)

add_library (ctamediachanger SHARED
  ${MEDIACHANGER_LIB_SRC_FILES})
set_property(TARGET ctamediachanger PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctamediachanger PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries (ctamediachanger
  ctacommon
)

install (TARGETS ctamediachanger DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

set (MEDIACHANGER_UNIT_TESTS_LIB_SRC_FILES
  IoTest.cpp
  LibrarySlotParserTest.cpp
  ScsiLibrarySlotTest.cpp)

add_library (ctamediachangerunittests SHARED
  ${MEDIACHANGER_UNIT_TESTS_LIB_SRC_FILES})
set_property(TARGET ctamediachangerunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctamediachangerunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries (ctamediachangerunittests
  ctamediachanger)

install (TARGETS ctamediachangerunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
