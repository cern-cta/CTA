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
# Rules to build and install acsd
################################################################################

add_subdirectory(daemon)

add_library(cta-acs 
  Acs.cpp
  AcsImpl.cpp
  AcsDebugBuf.cpp
  AcsLibraryInteraction.cpp)

target_link_libraries(cta-acs
  ctamediachanger 
  ctacommon
  ctareactor 
  zmq
  ${STK_LIBRARIES})

set_target_properties (cta-acs PROPERTIES
 COMPILE_FLAGS -I/usr/include/CDK
 COMPILE_DEFINITIONS LINUX)

################################################################################
# Rules to build and install castor-tape-acs-dismount
################################################################################
message(STATUS "Adding cta-tape-acs-dismount target")
set (ACS_DISMOUNT_SRC_FILES
  Acs.cpp
  AcsCmd.cpp
  AcsCmdLine.cpp
  AcsImpl.cpp
  AcsDismountCmd.cpp
  AcsDismountCmdLine.cpp
  AcsDismountCmdMain.cpp
  CmdLineTool.cpp
  AcsDebugBuf.cpp)
add_executable (cta-tape-acs-dismount ${ACS_DISMOUNT_SRC_FILES})
set_target_properties (cta-tape-acs-dismount PROPERTIES
  COMPILE_FLAGS -I/usr/include/CDK
  COMPILE_DEFINITIONS LINUX)
target_link_libraries (cta-tape-acs-dismount
  ctacommon
  ${STK_LIBRARIES})
install (TARGETS cta-tape-acs-dismount DESTINATION /usr/bin)
install (FILES cta-tape-acs-dismount.1cta DESTINATION /usr/share/man/man1)

################################################################################
# Rules to build and install cta-tape-acs-mount
################################################################################
set (ACS_MOUNT_SRC_FILES
  Acs.cpp
  AcsCmd.cpp
  AcsCmdLine.cpp
  AcsImpl.cpp
  AcsMountCmd.cpp
  AcsMountCmdLine.cpp
  AcsMountCmdMain.cpp
  CmdLineTool.cpp
  AcsDebugBuf.cpp)
add_executable (cta-tape-acs-mount ${ACS_MOUNT_SRC_FILES})
set_target_properties (cta-tape-acs-mount PROPERTIES
  COMPILE_FLAGS -I/usr/include/CDK
  COMPILE_DEFINITIONS LINUX)
target_link_libraries (cta-tape-acs-mount
ctacommon
${STK_LIBRARIES})
install (TARGETS cta-tape-acs-mount DESTINATION /usr/bin)
install (FILES cta-tape-acs-mount.1cta DESTINATION /usr/share/man/man1)

################################################################################
# Rules to build and install cta-tape-acs-queryvolume
################################################################################
message(STATUS "Adding cta-tape-acs-queryvolume target")
set (ACS_QUERYVOLUME_SRC_FILES
  Acs.cpp
  AcsCmd.cpp
  AcsCmdLine.cpp
  AcsImpl.cpp
  AcsQueryVolumeCmd.cpp
  AcsQueryVolumeCmdLine.cpp
  AcsQueryVolumeCmdMain.cpp
  CmdLineTool.cpp
  AcsDebugBuf.cpp)

add_executable (cta-tape-acs-queryvolume ${ACS_QUERYVOLUME_SRC_FILES})
target_link_libraries (cta-tape-acs-queryvolume
ctacommon
${STK_LIBRARIES})
set_target_properties (cta-tape-acs-queryvolume PROPERTIES
  COMPILE_FLAGS -I/usr/include/CDK
  COMPILE_DEFINITIONS LINUX)
install (TARGETS cta-tape-acs-queryvolume
  DESTINATION /usr/bin)
install (FILES cta-tape-acs-queryvolume.1cta DESTINATION /usr/share/man/man1)

################################################################################
# Rules to build and install cta-tape-acs-querydrive
################################################################################
message(STATUS "Adding cta-tape-acs-querydrive target")
set (ACS_QUERYDRIVE_SRC_FILES
  Acs.cpp
  AcsCmd.cpp
  AcsCmdLine.cpp
  AcsImpl.cpp
  AcsQueryDriveCmd.cpp
  AcsQueryDriveCmdLine.cpp
  AcsQueryDriveCmdMain.cpp
  CmdLineTool.cpp
  AcsDebugBuf.cpp)

add_executable (cta-tape-acs-querydrive ${ACS_QUERYDRIVE_SRC_FILES})
target_link_libraries (cta-tape-acs-querydrive
ctacommon
${STK_LIBRARIES})
set_target_properties (cta-tape-acs-querydrive PROPERTIES
  COMPILE_FLAGS -I/usr/include/CDK
  COMPILE_DEFINITIONS LINUX)
install (TARGETS cta-tape-acs-querydrive
  DESTINATION /usr/bin)
install (FILES cta-tape-acs-querydrive.1cta DESTINATION /usr/share/man/man1)
