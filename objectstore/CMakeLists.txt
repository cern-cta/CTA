cmake_minimum_required (VERSION 2.6)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../middletier)
include_directories(${CMAKE_BINARY_DIR})

find_package(Protobuf)

set (CTAProtoFiles
  cta.proto)

PROTOBUF_GENERATE_CPP(CTAProtoSources CTAProtoHeaders ${CTAProtoFiles})

add_library (CTAObjectStore SHARED
  ${CTAProtoSources}
  RootEntry.cpp
  Agent.cpp
  AgentRegister.cpp
  AgentWatchdog.cpp
  TapePool.cpp
  Tape.cpp
  ArchiveToFileRequest.cpp
  DriveRegister.cpp
  BackendVFS.cpp
  BackendRados.cpp
  ObjectOps.cpp
  ProtocolBuffersAlgorithms.cpp
  GenericObject.cpp
  GarbageCollector.cpp)

set(ObjectStoreUnitTests
  BackendTest.cpp
  RootEntryTest.cpp
  TapeTest.cpp
  GarbageCollectorTest.cpp
)

add_library(ctaobjectstoreunittests SHARED ${ObjectStoreUnitTests})
target_link_libraries(ctaobjectstoreunittests
  protobuf rados ctacommon gtest gmock)
