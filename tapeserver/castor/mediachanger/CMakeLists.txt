# This file is part of the Castor project.
# See http://castor.web.cern.ch/castor
#
# Copyright (C) 2003  CERN
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# @author Castor Dev team, castor-dev@cern.ch
#
cmake_minimum_required (VERSION 2.6)

include_directories(${CMAKE_SOURCE_DIR}/tapeserver)
include_directories(${PROJECT_BINARY_DIR}/tapeserver)

set (MEDIA_CHANGER_LIB_SRC_FILES
  AcsLibrarySlot.cpp
  LibrarySlot.cpp
  LibrarySlotParser.cpp
  ManualLibrarySlot.cpp
  MediaChangerFacade.cpp
  MmcProxy.cpp
  MmcProxyLog.cpp
  MmcProxyNotSupported.cpp
  ScsiLibrarySlot.cpp
  TapeLibraryType.cpp)
add_library(ctamediachanger ${MEDIA_CHANGER_LIB_SRC_FILES})

add_library(ctamediachangerutils SHARED
  MmcProxyDummy.cpp)
install(TARGETS ctamediachangerutils DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

set (MEDIA_CHANGER_MOUNT_SRC_FILES
  CmdLine.cpp
  CmdLineTool.cpp
  DebugBuf.cpp
  MountCmd.cpp
  MountCmdLine.cpp
  MountCmdMain.cpp)
add_executable (cta-mediachanger-mount ${MEDIA_CHANGER_MOUNT_SRC_FILES})
set_target_properties (cta-mediachanger-mount PROPERTIES
  COMPILE_FLAGS -I/usr/include/CDK
  COMPILE_DEFINITIONS LINUX)
target_link_libraries (cta-mediachanger-mount
  ctacommon
  ctalegacymsg
  ctamediachanger
  ctamessages
  ctautils
  ctaio)
install (TARGETS cta-mediachanger-mount DESTINATION /usr/bin)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-mediachanger-mount.1cta DESTINATION /usr/share/man/man1)

set (MEDIA_CHANGER_DISMOUNT_SRC_FILES
  CmdLine.cpp
  CmdLineTool.cpp
  DebugBuf.cpp
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
  ctalegacymsg
  ctamediachanger
  ctamessages
  ctautils
  ctaio)
install (TARGETS cta-mediachanger-dismount DESTINATION /usr/bin)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-mediachanger-dismount.1cta DESTINATION /usr/share/man/man1)
