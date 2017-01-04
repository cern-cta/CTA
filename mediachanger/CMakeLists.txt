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

find_package(openssl REQUIRED)
find_package(Protobuf REQUIRED)
find_package(zeromq REQUIRED)

file(GLOB ProtoFiles "${CMAKE_CURRENT_SOURCE_DIR}/*.proto")
PROTOBUF_GENERATE_CPP(ProtoSources ProtoHeaders ${ProtoFiles})

set (MEDIACHANGER_LIB_SRC_FILES
  AcsLibrarySlot.cpp
  AcsProxyZmq.cpp
  CmdLine.cpp
  CmdLineTool.cpp
  CommonMarshal.cpp
  DebugBuf.cpp
  Frame.cpp
  io.cpp
  IpAndPort.cpp
  LibrarySlot.cpp
  LibrarySlotParser.cpp
  ManualLibrarySlot.cpp
  MediaChangerFacade.cpp
  MediaChangerProxy.cpp
  MessageHeader.cpp
  messages.cpp
  MmcProxyLog.cpp
  MmcProxyNotSupported.cpp
  ${ProtoSources}
  RmcMarshal.cpp
  RmcMarshal.hpp
  RmcMountMsgBody.cpp
  RmcProxyTcpIp.cpp
  RmcUnmountMsgBody.cpp
  ScsiLibrarySlot.cpp
  SmartZmqContext.cpp
  TapeLibraryType.cpp
  ZmqContextSingleton.cpp
  ZmqMsg.cpp
  ZmqSocket.cpp
  ZmqSocketMT.cpp
  ZmqSocketST.cpp)

add_library (ctamediachanger SHARED
  ${MEDIACHANGER_LIB_SRC_FILES})

target_link_libraries (ctamediachanger
  ctacommon
  protobuf
  ssl
  zmq)

install (TARGETS ctamediachanger DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

set (MEDIACHANGER_UNIT_TESTS_LIB_SRC_FILES
  AcsLibrarySlotTest.cpp
  DismountCmdLine.cpp
  DismountCmdLineTest.cpp
  IoTest.cpp
  LibrarySlotParserTest.cpp
  ManualLibrarySlotTest.cpp
  MountCmdLine.cpp
  MountCmdLineTest.cpp
  ScsiLibrarySlotTest.cpp)

add_library (ctamediachangerunittests SHARED
  ${MEDIACHANGER_UNIT_TESTS_LIB_SRC_FILES})

target_link_libraries (ctamediachangerunittests
  ctamediachanger)

install (TARGETS ctamediachangerunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

set (MEDIA_CHANGER_MOUNT_SRC_FILES
  MountCmd.cpp
  MountCmdLine.cpp
  MountCmdMain.cpp)
add_executable (cta-mediachanger-mount ${MEDIA_CHANGER_MOUNT_SRC_FILES})
set_target_properties (cta-mediachanger-mount PROPERTIES
  COMPILE_FLAGS -I/usr/include/CDK
  COMPILE_DEFINITIONS LINUX)
target_link_libraries (cta-mediachanger-mount
  ctacommon
  ctamediachanger)
install (TARGETS cta-mediachanger-mount DESTINATION /usr/bin)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-mediachanger-mount.1cta DESTINATION /usr/share/man/man1)

set (MEDIA_CHANGER_DISMOUNT_SRC_FILES
  DismountCmd.cpp
  DismountCmdLine.cpp
  DismountCmdMain.cpp)
add_executable (cta-mediachanger-dismount
  ${MEDIA_CHANGER_DISMOUNT_SRC_FILES})
set_target_properties (cta-mediachanger-dismount PROPERTIES
  COMPILE_FLAGS -I/usr/include/CDK
  COMPILE_DEFINITIONS LINUX)
target_link_libraries (cta-mediachanger-dismount
  ctacommon
  ctamediachanger)
install (TARGETS cta-mediachanger-dismount DESTINATION /usr/bin)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-mediachanger-dismount.1cta DESTINATION /usr/share/man/man1)
