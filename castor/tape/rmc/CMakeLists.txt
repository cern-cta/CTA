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

################################################################################
# Rules to build and install castor-tape-acs-mount
################################################################################
set (ACS_MOUNT_SRC_FILES
  Acs.cpp
  AcsCmd.cpp
  AcsImpl.cpp
  AcsMountCmd.cpp
  AcsMountCmdLine.cpp
  AcsMountMain.cpp
  DebugBuf.cpp)
add_executable (castor-tape-acs-mount ${ACS_MOUNT_SRC_FILES})
set_target_properties (castor-tape-acs-mount PROPERTIES
  COMPILE_FLAGS -I/usr/include/CDK
  COMPILE_DEFINITIONS LINUX)
target_link_libraries (castor-tape-acs-mount castorcommon castortapeutils
  ${STK_API_LIB} ${STK_UTL_LIB} ${STK_IPC_LIB} ${STK_CL_LIB})
install (TARGETS castor-tape-acs-mount DESTINATION ${CASTOR_DEST_LIB_DIR})
CastorInstallExeManPage(castor-tape-acs-mount)

################################################################################
# Rules to build and install castor-tape-acs-dismount
################################################################################
set (ACS_DISMOUNT_SRC_FILES
  Acs.cpp
  AcsCmd.cpp
  AcsImpl.cpp
  AcsDismountCmd.cpp
  AcsDismountCmdLine.cpp
  AcsDismountMain.cpp
  DebugBuf.cpp)
add_executable (castor-tape-acs-dismount ${ACS_DISMOUNT_SRC_FILES})
target_link_libraries (castor-tape-acs-dismount castorcommon castortapeutils
  ${STK_API_LIB} ${STK_UTL_LIB} ${STK_IPC_LIB} ${STK_CL_LIB})
set_target_properties (castor-tape-acs-dismount PROPERTIES
  COMPILE_FLAGS -I/usr/include/CDK
  COMPILE_DEFINITIONS LINUX)
install (TARGETS castor-tape-acs-dismount DESTINATION ${CASTOR_DEST_LIB_DIR})
CastorInstallExeManPage(castor-tape-acs-dismount)

################################################################################
# Rules to build and install castor-tape-acs-queryvolume
################################################################################
set (ACS_QUERYVOLUME_SRC_FILES
  Acs.cpp
  AcsCmd.cpp
  AcsImpl.cpp
  AcsQueryVolumeCmd.cpp
  AcsQueryVolumeCmdLine.cpp
  AcsQueryVolumeMain.cpp
  DebugBuf.cpp)
add_executable (castor-tape-acs-queryvolume ${ACS_QUERYVOLUME_SRC_FILES})
target_link_libraries (castor-tape-acs-queryvolume castorcommon castortapeutils
  ${STK_API_LIB} ${STK_UTL_LIB} ${STK_IPC_LIB} ${STK_CL_LIB})
set_target_properties (castor-tape-acs-queryvolume PROPERTIES
  COMPILE_FLAGS -I/usr/include/CDK
  COMPILE_DEFINITIONS LINUX)
install (TARGETS castor-tape-acs-queryvolume DESTINATION ${CASTOR_DEST_LIB_DIR})
CastorInstallExeManPage(castor-tape-acs-queryvolume)
