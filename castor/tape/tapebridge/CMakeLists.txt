#
#                      castor/tape/tapebridge/CMakeLists.txt
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
# Rules to build and install tapebridged
################################################################################
set (TAPEBRIDGE_SRC_FILES
  BridgeClientInfo2Sender.cpp
  BridgeProtocolEngine.cpp
  BridgeSocketCatalogue.cpp
  BulkRequestConfigParams.cpp
  ClientAddress.cpp
  ClientAddressLocal.cpp
  ClientAddressTcpIp.cpp
  ClientProxy.cpp
  ConfigParams.cpp
  ConfigParamSource.cpp
  FileToMigrate.cpp
  FileToRecall.cpp
  GetMoreWorkConnection.cpp
  DlfMessageStrings.cpp
  IClientProxy.cpp
  IFileCloser.cpp
  ILegacyTxRx.cpp
  LegacyTxRx.cpp
  LogHelper.cpp
  MigrationReportConnection.cpp
  PendingMigrationsStore.cpp
  RtcpJobSubmitter.cpp
  RtcpTxRx.cpp
  SessionError.cpp
  SessionErrorList.cpp
  SessionException.cpp
  SystemFileCloser.cpp
  TapeBridgeDaemon.cpp
  TapeBridgeMain.cpp
  TapeFlushConfigParams.cpp
  VdqmRequestHandler.cpp
  VmgrTxRx.cpp)
add_executable (tapebridged ${TAPEBRIDGE_SRC_FILES})
target_link_libraries (tapebridged castorcommon castortapebridge
  castortapegatewayprotocol castortapelegacymsg castortapenet)
install (TARGETS tapebridged DESTINATION ${CASTOR_DEST_BIN_DIR})
CastorInstallAdmManPage (tapebridged)
CastorInstallLogrotate (castor-tapebridge-server)
CastorInstallSysconfig (tapebridged)
CastorInstallInitScript (tapebridged)
