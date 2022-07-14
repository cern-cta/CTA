# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2015-2022 CERN
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

add_subdirectory(castorrmc)
find_package(openssl REQUIRED)
find_package(Protobuf3 REQUIRED)

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
  ssl)

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

