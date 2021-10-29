# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2015-2021 CERN
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
cmake_minimum_required (VERSION 2.6)

include_directories (${CMAKE_CURRENT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR} ${CMAKE_BINARY_DIR} ${XROOTD_INCLUDE_DIR})

set (CTA_SCHEDULER_SRC_FILES
  ArchiveJob.cpp
  ArchiveMount.cpp
  DiskReportRunner.cpp
  DriveConfig.cpp
  LabelMount.cpp
  LogicalLibrary.cpp
  MountType.cpp
  MountType.cpp
  OStoreDB/MemQueues.cpp
  OStoreDB/OStoreDB.cpp
  OStoreDB/OStoreDBWithAgent.cpp
  OStoreDB/QueueItor.cpp
  PositioningMethod.cpp
  RepackReportThread.cpp
  RepackRequest.cpp
  RepackRequestManager.cpp
  RetrieveJob.cpp
  RetrieveMount.cpp
  Scheduler.cpp
  SchedulerDatabase.cpp
  SchedulerDatabaseFactory.cpp
  SchedulingInfos.cpp
  TapeDrivesCatalogueState.cpp
  TapeMount.cpp)

find_package(Protobuf3 REQUIRED)
include_directories (${PROTOBUF3_INCLUDE_DIRS})
add_library (ctascheduler SHARED
  ${CTA_SCHEDULER_SRC_FILES})
set_property(TARGET ctascheduler PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctascheduler PROPERTY   VERSION "${CTA_LIBVERSION}")

install (TARGETS ctascheduler DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

target_link_libraries (ctascheduler ctacommon ctaobjectstore ${PROTOBUF3_LIBRARIES} ctadisk)

#add_library (ctaschedulerutils SHARED
#  _old_prototype_DummyScheduler.cpp)

add_library (ctaschedulerunittests SHARED
  SchedulerDatabaseFactory.cpp
  SchedulerDatabaseTest.cpp
  SchedulerTest.cpp
  OStoreDB/OStoreDBTest.cpp)
set_property(TARGET ctaschedulerunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctaschedulerunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

if(CMAKE_COMPILER_IS_GNUCC)
  # Add -Wno-unused-function compiler flag if using gcc version 6 or greater
  if (GCC_VERSION VERSION_EQUAL 6 OR GCC_VERSION VERSION_GREATER 6)
    set_property(SOURCE OStoreDB/OStoreDBTest.cpp APPEND_STRING PROPERTY COMPILE_FLAGS " -Wno-unused-function")
    set_property(SOURCE SchedulerDatabaseTest.cpp APPEND_STRING PROPERTY COMPILE_FLAGS " -Wno-unused-function")
    set_property(SOURCE SchedulerTest.cpp APPEND_STRING PROPERTY COMPILE_FLAGS " -Wno-unused-function")
  endif (GCC_VERSION VERSION_EQUAL 6 OR GCC_VERSION VERSION_GREATER 6)
endif(CMAKE_COMPILER_IS_GNUCC)

target_link_libraries (ctaschedulerunittests
  ctascheduler)

install(TARGETS ctaschedulerunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
