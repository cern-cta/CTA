#
#                      castor/vdqm/CMakeLists.txt
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
# Rules to build and install vdqmd
################################################################################
set (VDQMD_SRC_FILES
  CDevTools.cpp
  DatabaseHelper.cpp
  DevTools.cpp
  DriveSchedulerThread.cpp
  OldProtocolInterpreter.cpp
  OldRequestFacade.cpp
  ProtocolFacade.cpp
  RequestHandlerThread.cpp
  RemoteCopyConnection.cpp
  RTCPJobSubmitterThread.cpp
  SocketHelper.cpp
  Utils.cpp
  VdqmDlfMessageStrings.cpp
  VdqmMagic2ProtocolInterpreter.cpp
  VdqmMagic3ProtocolInterpreter.cpp
  VdqmMagic4ProtocolInterpreter.cpp
  VdqmMain.cpp
  VdqmServer.cpp
  handler/BaseRequestHandler.cpp
  handler/TapeDriveConsistencyChecker.cpp
  handler/TapeDriveHandler.cpp
  handler/TapeDriveStatusHandler.cpp
  handler/TapeRequestHandler.cpp
  handler/VdqmMagic2RequestHandler.cpp
  handler/VdqmMagic3RequestHandler.cpp
  handler/VdqmMagic4RequestHandler.cpp)
add_executable (vdqmd ${VDQMD_SRC_FILES})
target_link_libraries (vdqmd castorclient castorcnvs castorcommon castorupv
  castorvdqmora castorvmgr)
install (TARGETS vdqmd DESTINATION ${CASTOR_DEST_BIN_DIR})
CastorInstallAdmManPage (vdqmd)
CastorInstallLogrotate (castor-vdqm2-server)
CastorInstallSysconfigExample (vdqmd)
CastorInstallInitScript (vdqmd)

################################################################################
# Rules to build and install vdqmDBInit
################################################################################
add_executable (vdqmDBInit vdqmDBInit.cpp)
target_link_libraries (vdqmDBInit castorcnvs castorcommon castorvmgr)
install (TARGETS vdqmDBInit DESTINATION ${CASTOR_DEST_BIN_DIR})
CastorInstallExeManPage (vdqmDBInit)
