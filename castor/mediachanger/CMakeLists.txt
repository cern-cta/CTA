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
#
# @author Castor Dev team, castor-dev@cern.ch
#
cmake_minimum_required (VERSION 2.6)

set (MEDIA_CHANGER_LIB_SRC_FILES
  AcsLibrarySlot.cpp
  GenericLibrarySlot.cpp
  ManualLibrarySlot.cpp
  MediaChangerFacade.cpp
  MmcProxy.cpp
  MmcProxyDummy.cpp
  MmcProxyLog.cpp
  MmcProxyNotSupported.cpp
  ScsiLibrarySlot.cpp
  TapeLibraryType.cpp)
add_library(castormediachanger ${MEDIA_CHANGER_LIB_SRC_FILES})

if (${COMPILE_SERVER} STREQUAL "1")

  ##############################################################################
  # Rules to build and install castor-tape-mediachanger-mount
  ##############################################################################
  set (MEDIA_CHANGER_MOUNT_SRC_FILES
    CmdLine.cpp
    CmdLineTool.cpp
    MountCmd.cpp
    MountCmdLine.cpp
    MountCmdMain.cpp)
  add_executable (castor-tape-mediachanger-mount
    ${MEDIA_CHANGER_MOUNT_SRC_FILES})
  set_target_properties (castor-tape-mediachanger-mount PROPERTIES
    COMPILE_FLAGS -I/usr/include/CDK
    COMPILE_DEFINITIONS LINUX)
  target_link_libraries (castor-tape-mediachanger-mount
    castorcommon
    castorlegacymsg
    castorlog
    castormediachanger
    castormessages
    castorutils
    ${STK_API_LIB}
    ${STK_UTL_LIB}
    ${STK_IPC_LIB}
    ${STK_CL_LIB})
  install (TARGETS castor-tape-mediachanger-mount
    DESTINATION ${CASTOR_DEST_BIN_DIR})
  CastorInstallExeManPage(castor-tape-mediachanger-mount)

  ##############################################################################
  # Rules to build and install castor-tape-mediachanger-dismount
  ##############################################################################
  set (MEDIA_CHANGER_DISMOUNT_SRC_FILES
    CmdLine.cpp
    CmdLineTool.cpp
    DismountCmd.cpp
    DismountCmdLine.cpp
    DismountCmdMain.cpp)
  add_executable (castor-tape-mediachanger-dismount
    ${MEDIA_CHANGER_DISMOUNT_SRC_FILES})
  set_target_properties (castor-tape-mediachanger-dismount PROPERTIES
    COMPILE_FLAGS -I/usr/include/CDK
    COMPILE_DEFINITIONS LINUX)
  target_link_libraries (castor-tape-mediachanger-dismount
    castorcommon
    castorlegacymsg
    castorlog
    castormediachanger
    castormessages
    castorutils
    ${STK_API_LIB}
    ${STK_UTL_LIB}
    ${STK_IPC_LIB}
    ${STK_CL_LIB})
  install (TARGETS castor-tape-mediachanger-dismount
    DESTINATION ${CASTOR_DEST_BIN_DIR})
  CastorInstallExeManPage(castor-tape-mediachanger-dismount)

endif (${COMPILE_SERVER} STREQUAL "1")
