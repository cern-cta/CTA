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

find_package(Protobuf REQUIRED)
find_package( ZLIB REQUIRED )

set(CTATAPESERVERDAEMON_LIBRARY_SRCS
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
  LabelSessionConfig.cpp
  DiskReadThreadPool.cpp
  DiskReadTask.cpp
  DiskWriteTask.cpp
  DiskWriteThreadPool.cpp
  DriveConfig.cpp
  DriveConfigMap.cpp
  EmptyDriveProbe.cpp
  TapeServerReporter.cpp
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

if(CMAKE_COMPILER_IS_GNUCC)
  if(GCC_VERSION_GE_4_8_0)
    foreach(CTATAPESERVERDAEMON_LIBRARY_SRC
       ${CTATAPESERVERDAEMON_LIBRARY_SRCS}
       CatalogueTest.cpp
       DataTransferSessionTest.cpp
       DiskReadTaskTest.cpp
       DiskWriteTaskTest.cpp
       DiskWriteTaskTest.cpp
       DiskWriteThreadPoolTest.cpp
       MigrationReportPackerTest.cpp
       ProcessForkerProxyDummy.cpp
       ProcessForkerTest.cpp
       RecallTaskInjectorTest.cpp
       TapeDaemon.cpp
       TapeDaemonTest.cpp)
      set_property(SOURCE ${CTATAPESERVERDAEMON_LIBRARY_SRC}
        PROPERTY COMPILE_FLAGS " -Wno-unused-local-typedefs")
    endforeach(CTATAPESERVERDAEMON_LIBRARY_SRC)
  endif(GCC_VERSION_GE_4_8_0)
endif(CMAKE_COMPILER_IS_GNUCC)

add_library(ctaTapeServerDaemon 
  ${CTATAPESERVERDAEMON_LIBRARY_SRCS})

target_link_libraries(ctaTapeServerDaemon ctamessages ctatapereactor  ctacommon ctaremotens protobuf ctascheduler ctalegacymsg ctacatalogue)
add_dependencies(ctaTapeServerDaemon ctamessagesprotobuf)

add_executable(cta-tapeserverd TapeDaemon.cpp)
target_link_libraries(cta-tapeserverd ctaTapeServerDaemon SCSI System Utils 
  File TapeDrive ctacommon ctatapereactor ${LIBCAP_LIB} ${ZLIB_LIBRARIES} 
  ctamessages zmq ctaio ctautils)
#install (TARGETS cta-tapeserverd DESTINATION usr/bin)

add_library(ctatapeserverdaemonutils SHARED
  ProcessForkerProxyDummy.cpp)

install(TARGETS ctatapeserverdaemonutils DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

add_library(ctatapeserverdaemonunittests SHARED
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

target_link_libraries(ctatapeserverdaemonunittests
  ctamediachangerutils
  ctamessagesutils
  ctatapeserverdaemonutils)
  #ctaschedulerutils

install(TARGETS ctatapeserverdaemonunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
