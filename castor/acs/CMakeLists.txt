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

################################################################################
# Try to find the four STK client libaries: libapi.so, libutl.so, libipc.so 
# and libcl.so
################################################################################
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
find_library (STK_CL_LIB cl PATHS /usr/lib64/CDK /usr/lib/CDK
  NO_DEFAULT_PATH)
if (STK_CL_LIB)
  message(STATUS "Found the STK cl library: ${STK_CL_LIB}")
endif (STK_CL_LIB)

################################################################################
# Add the acsd, acsdTest targets if all four STK client libaries
# (libapi.so, libutl.so, libipc.so and libcl.so) have been found
################################################################################
if (STK_API_LIB AND STK_UTL_LIB AND STK_IPC_LIB AND STK_CL_LIB)
  message(STATUS "Found all four STK libraries")

  ##############################################################################
  # Rules to build and install acsd
  ##############################################################################

  add_library(castorAcsDaemon 
    Acs.cpp
    AcsDaemon.cpp
    AcsDaemonConfig.cpp
    AcsDaemonMain.cpp
    AcsDismountTape.cpp
    AcsForceDismountTape.cpp
    AcsImpl.cpp
    AcsLibraryInteraction.cpp
    AcsMessageHandler.cpp
    AcsMountTapeReadOnly.cpp
    AcsMountTapeReadWrite.cpp
    AcsPendingRequests.cpp
    AcsRequest.cpp
    AcsRequestDismountTape.cpp)
  add_dependencies(castorAcsDaemon castormessagesprotobuf)

  target_link_libraries(castorAcsDaemon castortapereactor)
  set_target_properties (castorAcsDaemon PROPERTIES
    COMPILE_FLAGS -I/usr/include/CDK
    COMPILE_DEFINITIONS LINUX)


  add_executable(acsd AcsDaemon.cpp)
  set_target_properties (acsd PROPERTIES
    COMPILE_FLAGS -I/usr/include/CDK
    COMPILE_DEFINITIONS LINUX)

  target_link_libraries(acsd
    castorAcsDaemon
    castortapereactor
    castorcommon
    castorserver
    castormessages
    zmq
    ${STK_API_LIB}
    ${STK_UTL_LIB}
    ${STK_IPC_LIB}
    ${STK_CL_LIB})

  install (TARGETS acsd DESTINATION ${CASTOR_DEST_BIN_DIR})
  CastorInstallAdmManPage (acsd)
  CastorInstallLogrotate (castor-acs-server)
  CastorInstallSysconfigExample (acsd)
  CastorInstallInitScript (acsd)

  ##############################################################################
  # Rules to build and install castor-tape-acs-dismount
  ##############################################################################
  message(STATUS "Adding castor-tape-acs-dismount target")
  set (ACS_DISMOUNT_SRC_FILES
    Acs.cpp
    AcsCmd.cpp
    AcsCmdLine.cpp
    AcsImpl.cpp
    AcsDismountCmd.cpp
    AcsDismountCmdLine.cpp
    AcsDismountCmdMain.cpp
    CmdLineTool.cpp)
  add_executable (castor-tape-acs-dismount ${ACS_DISMOUNT_SRC_FILES})
  set_target_properties (castor-tape-acs-dismount PROPERTIES
    COMPILE_FLAGS -I/usr/include/CDK
    COMPILE_DEFINITIONS LINUX)
  target_link_libraries (castor-tape-acs-dismount
    castorclient
    castorcommon
    ${STK_API_LIB}
    ${STK_UTL_LIB}
    ${STK_IPC_LIB}
    ${STK_CL_LIB})
  install (TARGETS castor-tape-acs-dismount DESTINATION ${CASTOR_DEST_BIN_DIR})
  CastorInstallExeManPage(castor-tape-acs-dismount)

  ##############################################################################
  # Rules to build and install castor-tape-acs-mount
  ##############################################################################
  set (ACS_MOUNT_SRC_FILES
    Acs.cpp
    AcsCmd.cpp
    AcsCmdLine.cpp
    AcsImpl.cpp
    AcsMountCmd.cpp
    AcsMountCmdLine.cpp
    AcsMountCmdMain.cpp
    CmdLineTool.cpp)
  add_executable (castor-tape-acs-mount ${ACS_MOUNT_SRC_FILES})
  set_target_properties (castor-tape-acs-mount PROPERTIES
    COMPILE_FLAGS -I/usr/include/CDK
    COMPILE_DEFINITIONS LINUX)
  target_link_libraries (castor-tape-acs-mount
    castorclient
    castorcommon
    ${STK_API_LIB}
    ${STK_UTL_LIB}
    ${STK_IPC_LIB}
    ${STK_CL_LIB})
  install (TARGETS castor-tape-acs-mount DESTINATION ${CASTOR_DEST_BIN_DIR})
  CastorInstallExeManPage(castor-tape-acs-mount)

  ##############################################################################
  # Rules to build and install castor-tape-acs-queryvolume
  ##############################################################################
  message(STATUS "Adding castor-tape-acs-queryvolume target")
  set (ACS_QUERYVOLUME_SRC_FILES
    Acs.cpp
    AcsCmd.cpp
    AcsCmdLine.cpp
    AcsImpl.cpp
    AcsQueryVolumeCmd.cpp
    AcsQueryVolumeCmdLine.cpp
    AcsQueryVolumeCmdMain.cpp
    CmdLineTool.cpp)
  add_executable (castor-tape-acs-queryvolume ${ACS_QUERYVOLUME_SRC_FILES})
  target_link_libraries (castor-tape-acs-queryvolume
    castorclient
    castorcommon
    ${STK_API_LIB}
    ${STK_UTL_LIB}
    ${STK_IPC_LIB}
    ${STK_CL_LIB})
  set_target_properties (castor-tape-acs-queryvolume PROPERTIES
    COMPILE_FLAGS -I/usr/include/CDK
    COMPILE_DEFINITIONS LINUX)
  install (TARGETS castor-tape-acs-queryvolume
    DESTINATION ${CASTOR_DEST_BIN_DIR})
  CastorInstallExeManPage(castor-tape-acs-queryvolume)

endif (STK_API_LIB AND STK_UTL_LIB AND STK_IPC_LIB AND STK_CL_LIB)
