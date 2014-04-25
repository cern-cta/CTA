#
#                      castor/tape/rmc/CMakeLists.txt
#
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
# Steven.Murray@cern.ch
#
cmake_minimum_required (VERSION 2.6)

if (${COMPILE_SERVER} STREQUAL "1")
  ##############################################################################
  # Rules to build and install rmcd
  ##############################################################################
  set (RMCD_SRC_FILES
    RmcDaemon.cpp
    RmcdMain.cpp)
  add_executable (newrmcd ${RMCD_SRC_FILES})
  target_link_libraries (newrmcd castorclient castorcommon)
  #install (TARGETS newrmcd DESTINATION ${CASTOR_DEST_LIB_DIR})
  #CastorInstallExeManPage(newrmcd)

  ##############################################################################
  # Rules to build and install castor-tape-mount
  ##############################################################################
  set (MOUNT_SRC_FILES
    DebugBuf.cpp
    Cmd.cpp
    MountCmd.cpp
    MountCmdLine.cpp
    MountMain.cpp)
  add_executable (castor-tape-mount ${MOUNT_SRC_FILES})
  target_link_libraries (castor-tape-mount castorcommon castorrmc)
  #install (TARGETS castor-tape-mount DESTINATION ${CASTOR_DEST_LIB_DIR})
  #CastorInstallExeManPage(castor-tape-mount)

  ##############################################################################
  # Try to find the four STK client libaries: libapi.so, libutl.so, libipc.so 
  # and libcl.so
  ##############################################################################
  find_library (STK_API_LIB api
    PATHS /usr/lib64/CDK /usr/lib/CDK NO_DEFAULT_PATH)
  if (STK_API_LIB)
    message(STATUS "Found the STK api library: ${STK_API_LIB}")
  endif (STK_API_LIB)
  find_library (STK_UTL_LIB utl
    PATHS /usr/lib64/CDK /usr/lib/CDK NO_DEFAULT_PATH)
  if (STK_UTL_LIB)
    message(STATUS "Found the STK utl library: ${STK_UTL_LIB}")
  endif (STK_UTL_LIB)
  find_library (STK_IPC_LIB ipc
    PATHS /usr/lib64/CDK /usr/lib/CDK NO_DEFAULT_PATH)
  if (STK_IPC_LIB)
    message(STATUS "Found the STK ipc library: ${STK_IPC_LIB}")
  endif (STK_IPC_LIB)
  find_library (STK_CL_LIB cl PATHS /usr/lib64/CDK /usr/lib/CDK NO_DEFAULT_PATH)
  if (STK_CL_LIB)
    message(STATUS "Found the STK cl library: ${STK_CL_LIB}")
  endif (STK_CL_LIB)

  ##############################################################################
  # Add the castor-tape-acs-dismount, castor-tape-acs-mount and
  # castor-tape-queryvolume targets if all four STK client libaries
  # (libapi.so, libutl.so, libipc.so and libcl.so) have been found
  ##############################################################################
  if (STK_API_LIB AND STK_UTL_LIB AND STK_IPC_LIB AND STK_CL_LIB)
    message(STATUS "Found all four STK libraries")

    ############################################################################
    # Rules to build and install castor-tape-acs-dismount
    ############################################################################
    message(STATUS "Adding castor-tape-acs-dismount target")
    set (ACS_DISMOUNT_SRC_FILES
      Acs.cpp
      AcsCmd.cpp
      AcsImpl.cpp
      AcsDismountCmd.cpp
      AcsDismountCmdLine.cpp
      AcsDismountMain.cpp
      Cmd.cpp
      DebugBuf.cpp)
    add_executable (castor-tape-acs-dismount ${ACS_DISMOUNT_SRC_FILES})
    set_target_properties (castor-tape-acs-dismount PROPERTIES
      COMPILE_FLAGS -I/usr/include/CDK
      COMPILE_DEFINITIONS LINUX)
    target_link_libraries (castor-tape-acs-dismount
      castorclient
      castorcommon
      castortapeutils
      ${STK_API_LIB}
      ${STK_UTL_LIB}
      ${STK_IPC_LIB}
      ${STK_CL_LIB})
    #install (TARGETS castor-tape-acs-dismount
    #  DESTINATION ${CASTOR_DEST_LIB_DIR})
    #CastorInstallExeManPage(castor-tape-acs-dismount)

    ############################################################################
    # Rules to build and install castor-tape-acs-mount
    ############################################################################
    set (ACS_MOUNT_SRC_FILES
      Acs.cpp
      AcsCmd.cpp
      AcsImpl.cpp
      AcsMountCmd.cpp
      AcsMountCmdLine.cpp
      AcsMountMain.cpp
      Cmd.cpp
      DebugBuf.cpp)
    add_executable (castor-tape-acs-mount ${ACS_MOUNT_SRC_FILES})
    set_target_properties (castor-tape-acs-mount PROPERTIES
      COMPILE_FLAGS -I/usr/include/CDK
      COMPILE_DEFINITIONS LINUX)
    target_link_libraries (castor-tape-acs-mount
      castorclient
      castorcommon
      castortapeutils
      ${STK_API_LIB}
      ${STK_UTL_LIB}
      ${STK_IPC_LIB}
      ${STK_CL_LIB})
    #install (TARGETS castor-tape-acs-mount DESTINATION ${CASTOR_DEST_LIB_DIR})
    #CastorInstallExeManPage(castor-tape-acs-mount)

    ############################################################################
    # Rules to build and install castor-tape-acs-queryvolume
    ############################################################################
    message(STATUS "Adding castor-tape-acs-queryvolume target")
    set (ACS_QUERYVOLUME_SRC_FILES
      Acs.cpp
      AcsCmd.cpp
      AcsImpl.cpp
      AcsQueryVolumeCmd.cpp
      AcsQueryVolumeCmdLine.cpp
      AcsQueryVolumeMain.cpp
      Cmd.cpp
      DebugBuf.cpp)
    add_executable (castor-tape-acs-queryvolume ${ACS_QUERYVOLUME_SRC_FILES})
    target_link_libraries (castor-tape-acs-queryvolume
      castorclient
      castorcommon
      castortapeutils
      ${STK_API_LIB}
      ${STK_UTL_LIB}
      ${STK_IPC_LIB}
      ${STK_CL_LIB})
    set_target_properties (castor-tape-acs-queryvolume PROPERTIES
      COMPILE_FLAGS -I/usr/include/CDK
      COMPILE_DEFINITIONS LINUX)
    #install (TARGETS castor-tape-acs-queryvolume
    #  DESTINATION ${CASTOR_DEST_LIB_DIR})
    #CastorInstallExeManPage(castor-tape-acs-queryvolume)
  endif (STK_API_LIB AND STK_UTL_LIB AND STK_IPC_LIB AND STK_CL_LIB)
endif (${COMPILE_SERVER} STREQUAL "1")
