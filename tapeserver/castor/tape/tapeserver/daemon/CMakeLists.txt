# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2003-2021 CERN
# @license        This program is free software: you can redistribute it and/or modify
#                 it under the terms of the GNU General Public License as published by
#                 the Free Software Foundation, either version 3 of the License, or
#                 (at your option) any later version.
#
#                 This program is distributed in the hope that it will be useful,
#                 but WITHOUT ANY WARRANTY; without even the implied warranty of
#                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#                 GNU General Public License for more details.
#
#                 You should have received a copy of the GNU General Public License
#                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
cmake_minimum_required (VERSION 3.17)

include_directories(/usr/include/shift)
include_directories(${CMAKE_SOURCE_DIR}/tapeserver)
include_directories(${CMAKE_SOURCE_DIR}/tapeserver/h)
include_directories(${PROJECT_BINARY_DIR}/tapeserver)
include_directories(${CMAKE_SOURCE_DIR})
include_directories(${PROJECT_BINARY_DIR})

find_package(Protobuf3 REQUIRED)
find_package( ZLIB REQUIRED )

set(CTATAPESERVERDAEMON_LIBRARY_SRCS
  CleanerSession.cpp
  DataTransferConfig.cpp
  LabelSessionConfig.cpp
  DiskReadThreadPool.cpp
  DiskReadTask.cpp
  DiskWriteTask.cpp
  DiskWriteThreadPool.cpp
  EmptyDriveProbe.cpp
  EncryptionControl.cpp
  TapeServerReporter.cpp
  LabelSession.cpp
  MigrationMemoryManager.cpp
  MigrationReportPacker.cpp
  MigrationTaskInjector.cpp
  DataTransferSession.cpp
  RecallMemoryManager.cpp
  RecallTaskInjector.cpp
  RecallReportPacker.cpp
  Session.cpp
  TapeReadSingleThread.cpp
  TapeWriteSingleThread.cpp
  TapeWriteTask.cpp)

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
       RecallTaskInjectorTest.cpp)
      set_property(SOURCE ${CTATAPESERVERDAEMON_LIBRARY_SRC}
        PROPERTY COMPILE_FLAGS " -Wno-unused-local-typedefs")
    endforeach(CTATAPESERVERDAEMON_LIBRARY_SRC)

    # Add -Wno-unused-function compiler flag if using gcc version 6 or greater
    if (GCC_VERSION VERSION_EQUAL 6 OR GCC_VERSION VERSION_GREATER 6)
      set_property(SOURCE DataTransferSessionTest.cpp APPEND_STRING PROPERTY COMPILE_FLAGS " -Wno-unused-function")
    endif (GCC_VERSION VERSION_EQUAL 6 OR GCC_VERSION VERSION_GREATER 6)
  endif(GCC_VERSION_GE_4_8_0)
endif(CMAKE_COMPILER_IS_GNUCC)

add_library(ctaTapeServerDaemon
  ${CTATAPESERVERDAEMON_LIBRARY_SRCS})

target_link_libraries(ctaTapeServerDaemon ctamessages ctacommon ${PROTOBUF3_LIBRARIES} ctascheduler ctalegacymsg ctacatalogue TapeDrive ctarao)
add_dependencies(ctaTapeServerDaemon ctamessagesprotobuf)

include_directories(${PROTOBUF3_INCLUDE_DIRS})
add_library(ctatapeserverdaemonunittests SHARED
  DataTransferSessionTest.cpp
  DiskReadTaskTest.cpp
  DiskWriteTaskTest.cpp
  DiskWriteThreadPoolTest.cpp
  MigrationReportPackerTest.cpp
  RecallReportPackerTest.cpp
  RecallTaskInjectorTest.cpp
  TaskWatchDogTest.cpp
  #${CMAKE_BINARY_DIR}/catalogue/OracleCatalogueSchema.cpp
)
set_property(TARGET ctatapeserverdaemonunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctatapeserverdaemonunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

if(CMAKE_COMPILER_IS_GNUCC)
  # Add -Wno-unused-function compiler flag if using gcc version 6 or greater
  if (GCC_VERSION VERSION_EQUAL 6 OR GCC_VERSION VERSION_GREATER 6)
    set_property(SOURCE RecallReportPackerTest.cpp APPEND_STRING PROPERTY COMPILE_FLAGS " -Wno-unused-function")
  endif (GCC_VERSION VERSION_EQUAL 6 OR GCC_VERSION VERSION_GREATER 6)
endif(CMAKE_COMPILER_IS_GNUCC)

target_link_libraries(ctatapeserverdaemonunittests
  ctamessagesutils)
  #ctaschedulerutils

install(TARGETS ctatapeserverdaemonunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
