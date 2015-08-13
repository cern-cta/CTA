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

include_directories(/usr/include/shift)
include_directories(${CMAKE_SOURCE_DIR}/tapeserver)
include_directories(${CMAKE_SOURCE_DIR}/tapeserver/h)
include_directories(${PROJECT_BINARY_DIR}/tapeserver)
include_directories(${CMAKE_SOURCE_DIR})
include_directories(${PROJECT_BINARY_DIR})

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
  CleanerSession.cpp
  DataTransferConfig.cpp
  DiskReadThreadPool.cpp
  DiskReadTask.cpp
  DiskWriteTask.cpp
  DiskWriteThreadPool.cpp
  DriveConfig.cpp
  DriveConfigMap.cpp
  EmptyDriveProbe.cpp
  TapeServerReporter.cpp
  LabelCmdAcceptHandler.cpp
  LabelCmdConnectionHandler.cpp
  LabelSession.cpp
  MigrationMemoryManager.cpp
  MigrationReportPacker.cpp 
  MigrationTaskInjector.cpp
  DataTransferSession.cpp
  ProcessForker.cpp
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
  TpconfigLines.cpp)

target_link_libraries(castorTapeServerDaemon castormessages castortapereactor  ctacommon ctanameserver ctaremotens ctaOStoreSchedulerDB protobuf CTAObjectStore ctascheduler)
add_dependencies(castorTapeServerDaemon castormessagesprotobuf)

add_library(castorTapeServerDaemonTestDummies
  ../client/ClientSimulator.cpp 
  ../../tpcp/TpcpCommand.cpp
  ../../tpcp/StreamOperators.cpp
  ../../tpcp/Helper.cpp
  ../../tpcp/TapeFseqRange.cpp)

add_executable(tapeserverd TapeDaemon.cpp)
target_link_libraries(tapeserverd castorTapeServerDaemon SCSI System Utils File TapeDrive castorcommon castorclient castorlegacymsg castorserver castortapegatewayprotocol castortapereactor ${LIBCAP_LIB} ${ZLIB_LIBRARIES} castormessages zmq)

# With the exception of shared-library plugins, the CASTOR rpms only install the
# /usr/lib64/libcastor*.so symbolic links for libraries used by end-user
# developers.  Therefore the locations of the internal CASTOR libraries required
# by tapeserved and not by end-user developers need to be imported into cmake.
add_library(castorlegacymsg SHARED IMPORTED)
set_target_properties(castorlegacymsg PROPERTIES
  IMPORTED_LOCATION /usr/lib64/libcastorlegacymsg.so.2.1)
add_library(castorserver SHARED IMPORTED)
set_target_properties(castorserver PROPERTIES
  IMPORTED_LOCATION /usr/lib64/libcastorserver.so.2.1)
add_library(castortapegatewayprotocol SHARED IMPORTED)
set_target_properties(castortapegatewayprotocol PROPERTIES
  IMPORTED_LOCATION /usr/lib64/libcastortapegatewayprotocol.so.2.1)
