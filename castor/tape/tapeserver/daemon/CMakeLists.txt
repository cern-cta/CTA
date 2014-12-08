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
find_package( ZLIB REQUIRED )

add_library(castorTapeServerDaemon 
  AdminAcceptHandler.cpp
  AdminConnectionHandler.cpp
  ../client/ClientProxy.cpp 
  Catalogue.cpp
  CatalogueDrive.cpp
  CatalogueDriveState.cpp
  CatalogueSession.cpp
  CatalogueLabelSession.cpp
  CatalogueTransferSession.cpp
  CatalogueCleanerSession.cpp
  CatalogueConfig.cpp
  DataTransferConfig.cpp
  DiskReadThreadPool.cpp
  DiskReadTask.cpp
  DiskWriteTask.cpp
  DiskWriteThreadPool.cpp
  DriveConfig.cpp
  DriveConfigMap.cpp
  TapeServerReporter.cpp
  LabelCmdAcceptHandler.cpp
  LabelCmdConnectionHandler.cpp
  LabelSession.cpp
  CleanerSession.cpp
  MigrationMemoryManager.cpp
  MigrationReportPacker.cpp 
  MigrationTaskInjector.cpp
  DataTransferSession.cpp
  ProcessForker.cpp
  ProcessForkerConfig.cpp
  ProcessForkerConnectionHandler.cpp
  ProcessForkerProxy.cpp
  ProcessForkerProxyDummy.cpp
  ProcessForkerProxySocket.cpp
  ProcessForkerUtils.cpp
  RecallMemoryManager.cpp
  RecallTaskInjector.cpp 
  RecallReportPacker.cpp
  Session.cpp
  TapeMessageHandler.cpp
  TapeDaemonConfig.cpp
  TapeDaemonMain.cpp
  TapeReadSingleThread.cpp
  TapeWriteSingleThread.cpp
  TapeWriteTask.cpp
  TpconfigLine.cpp
  TpconfigLines.cpp
  VdqmAcceptHandler.cpp
  VdqmConnectionHandler.cpp
  VdqmDriveSynchronizer.cpp)

target_link_libraries(castorTapeServerDaemon castormessages castortapereactor)
add_dependencies(castorTapeServerDaemon castormessagesprotobuf)

add_library(castorTapeServerDaemonTestDummies
  ../client/ClientSimulator.cpp 
  ../../tpcp/TpcpCommand.cpp
  ../../tpcp/StreamOperators.cpp
  ../../tpcp/Helper.cpp
  ../../tpcp/TapeFseqRange.cpp)

add_executable(tapeserverd TapeDaemon.cpp)

target_link_libraries(tapeserverd
  castorTapeServerDaemon
  SCSI
  System
  Utils
  File
  TapeDrive
  castorcommon
  castorclient
  castorlegacymsg
  castorserver
  castortapegatewayprotocol
  castortapereactor
  ${LIBCAP_LIB}
  ${ZLIB_LIBRARIES}
  castormessages
  zmq)

install (TARGETS tapeserverd DESTINATION ${CASTOR_DEST_BIN_DIR})
CastorInstallAdmManPage (tapeserverd)
CastorInstallLogrotate (castor-tapeserver-server)
CastorInstallSysconfigExample (tapeserverd)
CastorInstallInitScript (tapeserverd)
CastorInstallUdevRule (99-tape.rules)
