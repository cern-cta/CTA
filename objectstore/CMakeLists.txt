# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

cmake_minimum_required (VERSION 3.17)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../middletier)

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
  GarbageCollector.cpp
  QueueCleanup.cpp
  BackendVFS.cpp
  BackendRados.cpp
  BackendPopulator.cpp
  BackendFactory.cpp
  ProtocolBuffersAlgorithms.cpp
  GenericObject.cpp
  SchedulerGlobalLock.cpp
  ValueCountMap.cpp
  Helpers.cpp
  RetrieveActivityCountMap.cpp)
set_property(TARGET ctaobjectstore PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctaobjectstore PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries(ctaobjectstore rados cryptopp)
set_source_files_properties(BackendRados.cpp PROPERTIES COMPILE_FLAGS -Wno-deprecated-declarations)
if(NOT CTA_USE_PGSCHED)
  install (TARGETS ctaobjectstore DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
endif()

set(ObjectStoreUnitTests
  ObjectStoreFixture.cpp
  BackendTest.cpp
  GarbageCollectorTest.cpp
  QueueCleanupTestUtils.cpp
  QueueCleanupTest.cpp
  QueueCleanupConcurrentTest.cpp
  RootEntryTest.cpp
  RetrieveQueueTest.cpp
  AlgorithmsTest.cpp
  SorterTest.cpp
)

add_library(ctaobjectstoreunittests SHARED ${ObjectStoreUnitTests})
set_property(TARGET ctaobjectstoreunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctaobjectstoreunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries(ctaobjectstoreunittests
  rados ctacommon gtest ctaobjectstore)
if(NOT CTA_USE_PGSCHED)
  install(TARGETS ctaobjectstoreunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
endif()

add_executable(cta-objectstore-initialize cta-objectstore-initialize.cpp)
target_link_libraries(cta-objectstore-initialize
  ctaobjectstore ctacommon)

add_executable(cta-objectstore-list cta-objectstore-list.cpp)
target_link_libraries(cta-objectstore-list
  ctaobjectstore ctacommon)

add_executable(cta-objectstore-dump-object cta-objectstore-dump-object.cpp)
target_link_libraries(cta-objectstore-dump-object
  ctaobjectstore ctacommon)

add_executable(cta-objectstore-dereference-removed-queues cta-objectstore-dereference-removed-queues.cpp)
target_link_libraries(cta-objectstore-dereference-removed-queues
  ctaobjectstore ctacommon)

add_executable(cta-objectstore-collect-orphaned-object cta-objectstore-collect-orphaned-object.cpp)
target_link_libraries(cta-objectstore-collect-orphaned-object
  ctaobjectstore ctacommon ctacatalogue)

add_executable(cta-objectstore-create-missing-repack-index cta-objectstore-create-missing-repack-index.cpp)
target_link_libraries(cta-objectstore-create-missing-repack-index
  ctaobjectstore ctacommon)

if(NOT CTA_USE_PGSCHED)
  install(TARGETS cta-objectstore-initialize cta-objectstore-list cta-objectstore-dump-object
    cta-objectstore-dereference-removed-queues cta-objectstore-collect-orphaned-object cta-objectstore-create-missing-repack-index
    DESTINATION usr/bin)

  install(FILES cta-objectstore-tools.conf.example
    DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta
    PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)
endif()
