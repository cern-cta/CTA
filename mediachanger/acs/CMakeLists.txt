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
# Rules to build and install unit-tests
################################################################################

set(MEDIACHANGER_ACS_UNIT_TESTS_LIB_SRC_FILES
  AcsCmdLine.cpp
  AcsCmdLineTest.cpp
  AcsTest.cpp)

add_library (ctamediachangeracsunittests SHARED
  ${MEDIACHANGER_ACS_UNIT_TESTS_LIB_SRC_FILES})
set_property(TARGET ctamediachangeracsunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctamediachangeracsunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries (ctamediachangeracsunittests
  cta-acs)

set_target_properties (ctamediachangeracsunittests PROPERTIES
 COMPILE_FLAGS -I/usr/include/CDK
 COMPILE_DEFINITIONS LINUX)

install(TARGETS ctamediachangeracsunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

################################################################################
# Rules to build and install castor-tape-acs-dismount
################################################################################
message(STATUS "Adding cta-acs-dismount target")
set (ACS_DISMOUNT_SRC_FILES
  ../../common/exception/Backtrace.cpp
  ../../common/exception/Exception.cpp
  ../../common/exception/Mismatch.cpp
  ../../common/exception/MissingOperand.cpp
  ../../common/exception/RequestFailed.cpp
  Acs.cpp
  AcsCmd.cpp
  AcsCmdLine.cpp
  AcsImpl.cpp
  AcsDismountCmd.cpp
  AcsDismountCmdLine.cpp
  AcsDismountCmdMain.cpp
  CmdLineTool.cpp
  AcsDebugBuf.cpp)
add_executable (cta-acs-dismount ${ACS_DISMOUNT_SRC_FILES})
set_target_properties (cta-acs-dismount PROPERTIES
  COMPILE_FLAGS -I/usr/include/CDK
  COMPILE_DEFINITIONS LINUX)
target_link_libraries (cta-acs-dismount
  ${STK_LIBRARIES})
install (TARGETS cta-acs-dismount DESTINATION /usr/bin)
install (FILES cta-acs-dismount.1cta DESTINATION /usr/share/man/man1)

################################################################################
# Rules to build and install cta-acs-mount
################################################################################
set (ACS_MOUNT_SRC_FILES
  ../../common/exception/Backtrace.cpp
  ../../common/exception/Exception.cpp
  ../../common/exception/Mismatch.cpp
  ../../common/exception/MissingOperand.cpp
  ../../common/exception/RequestFailed.cpp
  Acs.cpp
  AcsCmd.cpp
  AcsCmdLine.cpp
  AcsImpl.cpp
  AcsMountCmd.cpp
  AcsMountCmdLine.cpp
  AcsMountCmdMain.cpp
  CmdLineTool.cpp
  AcsDebugBuf.cpp)
add_executable (cta-acs-mount ${ACS_MOUNT_SRC_FILES})
set_target_properties (cta-acs-mount PROPERTIES
  COMPILE_FLAGS -I/usr/include/CDK
  COMPILE_DEFINITIONS LINUX)
target_link_libraries (cta-acs-mount
  ${STK_LIBRARIES})
install (TARGETS cta-acs-mount DESTINATION /usr/bin)
install (FILES cta-acs-mount.1cta DESTINATION /usr/share/man/man1)

################################################################################
# Rules to build and install cta-acs-queryvolume
################################################################################
message(STATUS "Adding cta-acs-queryvolume target")
set (ACS_QUERYVOLUME_SRC_FILES
  ../../common/exception/Backtrace.cpp
  ../../common/exception/Exception.cpp
  ../../common/exception/Mismatch.cpp
  ../../common/exception/MissingOperand.cpp
  ../../common/exception/RequestFailed.cpp
  Acs.cpp
  AcsCmd.cpp
  AcsCmdLine.cpp
  AcsImpl.cpp
  AcsQueryVolumeCmd.cpp
  AcsQueryVolumeCmdLine.cpp
  AcsQueryVolumeCmdMain.cpp
  CmdLineTool.cpp
  AcsDebugBuf.cpp)

add_executable (cta-acs-queryvolume ${ACS_QUERYVOLUME_SRC_FILES})
target_link_libraries (cta-acs-queryvolume
  ${STK_LIBRARIES})
set_target_properties (cta-acs-queryvolume PROPERTIES
  COMPILE_FLAGS -I/usr/include/CDK
  COMPILE_DEFINITIONS LINUX)
install (TARGETS cta-acs-queryvolume
  DESTINATION /usr/bin)
install (FILES cta-acs-queryvolume.1cta DESTINATION /usr/share/man/man1)

################################################################################
# Rules to build and install cta-acs-querydrive
################################################################################
message(STATUS "Adding cta-acs-querydrive target")
set (ACS_QUERYDRIVE_SRC_FILES
  ../../common/exception/Backtrace.cpp
  ../../common/exception/Exception.cpp
  ../../common/exception/Mismatch.cpp
  ../../common/exception/MissingOperand.cpp
  ../../common/exception/RequestFailed.cpp
  Acs.cpp
  AcsCmd.cpp
  AcsCmdLine.cpp
  AcsImpl.cpp
  AcsQueryDriveCmd.cpp
  AcsQueryDriveCmdLine.cpp
  AcsQueryDriveCmdMain.cpp
  CmdLineTool.cpp
  AcsDebugBuf.cpp)

add_executable (cta-acs-querydrive ${ACS_QUERYDRIVE_SRC_FILES})
target_link_libraries (cta-acs-querydrive
  ${STK_LIBRARIES})
set_target_properties (cta-acs-querydrive PROPERTIES
  COMPILE_FLAGS -I/usr/include/CDK
  COMPILE_DEFINITIONS LINUX)
install (TARGETS cta-acs-querydrive
  DESTINATION /usr/bin)
install (FILES cta-acs-querydrive.1cta DESTINATION /usr/share/man/man1)
