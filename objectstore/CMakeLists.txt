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

cmake_minimum_required (VERSION 3.17)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../middletier)


find_package(json-c REQUIRED)
find_package(librados2 REQUIRED)
find_package(Protobuf3 REQUIRED)

set (CTAProtoFiles
  cta.proto)

PROTOBUF3_GENERATE_CPP(CTAProtoSources CTAProtoHeaders ${CTAProtoFiles})

set (CTAProtoDependants
  objectstore/Agent.hpp
  objectstore/ArchiveRequest.hpp
  objectstore/CreationLog.hpp
  objectstore/DriveRegister.hpp
  objectstore/GenericObject.hpp
  objectstore/ObjectOps.hpp
  objectstore/RepackIndex.hpp
  objectstore/RepackRequest.hpp
  objectstore/RepackQueue.hpp
  objectstore/RetrieveActivityCountMap.hpp
  objectstore/RetrieveRequest.hpp
  objectstore/RootEntry.hpp
  objectstore/SchedulerGlobalLock.hpp
  objectstore/TapeQueue.hpp
  objectstore/ArchiveQueue.hpp
  objectstore/UserIdentity.hpp
  objectstore/ValueCountMap.hpp)

SET_SOURCE_FILES_PROPERTIES(${CTAProtoHeaders} PROPERTIES HEADER_FILE_ONLY TRUE)
SET_SOURCE_FILES_PROPERTIES(${CTAProtoDependants}
  PROPERTIES OBJECT_DEPENDS ${CTAProtoHeaders})

include_directories (${PROTOBUF3_INCLUDE_DIRS})
add_library (ctaobjectstore SHARED
  ${CTAProtoSources}
  ObjectOps.cpp
  RootEntry.cpp
  Agent.cpp
  AgentHeartbeatThread.cpp
  AgentReference.cpp
  AgentReferenceInterface.cpp
  AgentWrapper.cpp
  AgentRegister.cpp
  AgentWatchdog.cpp
  ArchiveQueue.cpp
  ArchiveQueueShard.cpp
  ArchiveQueueToTransferAlgorithms.cpp
  ArchiveQueueToReportAlgorithms.cpp
  ArchiveQueueFailedAlgorithms.cpp
  ArchiveQueueToReportToRepackForSuccessAlgorithms.cpp
  ArchiveQueueToReportToRepackForFailureAlgorithms.cpp
  ArchiveQueueToTransferForRepackAlgorithms.cpp
  RetrieveQueue.cpp
  RetrieveQueueShard.cpp
  RetrieveQueueToTransferAlgorithms.cpp
  RetrieveQueueToReportAlgorithms.cpp
  RetrieveQueueFailedAlgorithms.cpp
  RetrieveQueueToReportToRepackForSuccessAlgorithms.cpp
  RetrieveQueueToReportToRepackForFailureAlgorithms.cpp
  Sorter.cpp
  ArchiveRequest.cpp
  RetrieveRequest.cpp
  DriveRegister.cpp
  RepackIndex.cpp
  RepackRequest.cpp
  RepackQueue.cpp
  RepackQueuePendingAlgorithms.cpp
  RepackQueueToExpandAlgorithms.cpp
  BackendVFS.cpp
  BackendRados.cpp
  BackendPopulator.cpp
  BackendFactory.cpp
  ProtocolBuffersAlgorithms.cpp
  GenericObject.cpp
  GarbageCollector.cpp
  SchedulerGlobalLock.cpp
  ValueCountMap.cpp
  Helpers.cpp
  RetrieveActivityCountMap.cpp)
set_property(TARGET ctaobjectstore PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctaobjectstore PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries(ctaobjectstore rados json-c cryptopp)
set_source_files_properties(BackendRados.cpp PROPERTIES COMPILE_FLAGS -Wno-deprecated-declarations)
install (TARGETS ctaobjectstore DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

set(ObjectStoreUnitTests
  BackendTest.cpp
  RootEntryTest.cpp
  RetrieveQueueTest.cpp
  GarbageCollectorTest.cpp
  AlgorithmsTest.cpp
  SorterTest.cpp
)

add_library(ctaobjectstoreunittests SHARED ${ObjectStoreUnitTests})
set_property(TARGET ctaobjectstoreunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctaobjectstoreunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries(ctaobjectstoreunittests
  ${PROTOBUF3_LIBRARIES} rados ctacommon gtest ctaobjectstore)
install(TARGETS ctaobjectstoreunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

add_executable(cta-objectstore-initialize cta-objectstore-initialize.cpp)
set_property(TARGET cta-objectstore-initialize APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})
target_link_libraries(cta-objectstore-initialize
  ${PROTOBUF3_LIBRARIES} ctaobjectstore ctacommon)

add_executable(cta-objectstore-list cta-objectstore-list.cpp)
set_property(TARGET cta-objectstore-list APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})
target_link_libraries(cta-objectstore-list
  ${PROTOBUF3_LIBRARIES} ctaobjectstore ctacommon)

add_executable(cta-objectstore-dump-object cta-objectstore-dump-object.cpp)
set_property(TARGET cta-objectstore-dump-object APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})
target_link_libraries(cta-objectstore-dump-object
  ${PROTOBUF3_LIBRARIES} ctaobjectstore ctacommon)

add_executable(cta-objectstore-dereference-removed-queues cta-objectstore-dereference-removed-queues.cpp)
set_property(TARGET cta-objectstore-dereference-removed-queues APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})
target_link_libraries(cta-objectstore-dereference-removed-queues
  ${PROTOBUF3_LIBRARIES} ctaobjectstore ctacommon)

add_executable(cta-objectstore-collect-orphaned-object cta-objectstore-collect-orphaned-object.cpp)
set_property(TARGET cta-objectstore-collect-orphaned-object APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})
target_link_libraries(cta-objectstore-collect-orphaned-object
  ${PROTOBUF3_LIBRARIES} ctaobjectstore ctacommon ctacatalogue)

add_executable(cta-objectstore-create-missing-repack-index cta-objectstore-create-missing-repack-index.cpp)
set_property(TARGET cta-objectstore-create-missing-repack-index APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})
target_link_libraries(cta-objectstore-create-missing-repack-index
  ${PROTOBUF3_LIBRARIES} ctaobjectstore ctacommon)

install(TARGETS cta-objectstore-initialize cta-objectstore-list cta-objectstore-dump-object
  cta-objectstore-dereference-removed-queues cta-objectstore-collect-orphaned-object cta-objectstore-create-missing-repack-index
  DESTINATION usr/bin)
