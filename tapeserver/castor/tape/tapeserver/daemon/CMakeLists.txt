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

target_link_libraries(castorTapeServerDaemon castormessages castortapereactor  ctacommon ctanameserver ctaremotens protobuf ctascheduler)
add_dependencies(castorTapeServerDaemon castormessagesprotobuf)

add_executable(tapeserverd TapeDaemon.cpp)
target_link_libraries(tapeserverd castorTapeServerDaemon SCSI System Utils File TapeDrive castorcommon castorclient castorlegacymsg castorserver castortapereactor ${LIBCAP_LIB} ${ZLIB_LIBRARIES} castormessages zmq)
install (TARGETS tapeserverd DESTINATION usr/bin)

add_library(castortapeserverdaemonutils SHARED
  ProcessForkerProxyDummy.cpp)

add_library(castortapeserverdaemonunittests SHARED
  CatalogueDriveStateTest.cpp
  CatalogueTest.cpp
  DataTransferSessionTest.cpp
  DiskReadTaskTest.cpp
  DiskWriteTaskTest.cpp
  DiskWriteThreadPoolTest.cpp
  DriveConfigTest.cpp
  MigrationReportPackerTest.cpp
  ProcessForkerTest.cpp
  RecallReportPackerTest.cpp
  RecallTaskInjectorTest.cpp
  TapeDaemon.cpp
  TapeDaemonTest.cpp
  TaskWatchDogTest.cpp
)

target_link_libraries(castortapeserverdaemonunittests
  castormediachangerutils
  castormessagesutils
  castorserverutils
  castortapeserverdaemonutils
  ctaschedulerutils)
