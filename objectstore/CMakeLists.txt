cmake_minimum_required (VERSION 2.6)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../middletier)


find_package(Protobuf)

set (CTAProtoFiles
  cta.proto)

PROTOBUF_GENERATE_CPP(CTAProtoSources CTAProtoHeaders ${CTAProtoFiles})

set (CTAProtoDependants objectstore/Agent.hpp
  objectstore/ArchiveToFileRequest.hpp
  objectstore/CreationLog.hpp
  objectstore/GenericObject.hpp
  objectstore/ObjectOps.cpp
  objectstore/ObjectOps.hpp
  objectstore/RetrieveToFileRequest.cpp
  objectstore/RetrieveToFileRequest.hpp
  objectstore/RootEntry.hpp
  objectstore/SchedulerGlobalLock.hpp
  objectstore/Tape.hpp
  objectstore/TapePool.hpp
  objectstore/UserIdentity.hpp)

SET_SOURCE_FILES_PROPERTIES(${CTAProtoHeaders} PROPERTIES HEADER_FILE_ONLY TRUE)
SET_SOURCE_FILES_PROPERTIES(${CTAProtoDependants} 
  PROPERTIES OBJECT_DEPENDS ${CTAProtoHeaders})


add_library (CTAObjectStore
  ${CTAProtoSources}
  RootEntry.cpp
  Agent.cpp
  AgentRegister.cpp
  AgentWatchdog.cpp
  TapePool.cpp
  Tape.cpp
  ArchiveToFileRequest.cpp
  RetrieveToFileRequest.cpp
  DriveRegister.cpp
  BackendVFS.cpp
  BackendRados.cpp
  BackendFactory.cpp
  ObjectOps.cpp
  ProtocolBuffersAlgorithms.cpp
  GenericObject.cpp
  GarbageCollector.cpp
  SchedulerGlobalLock.cpp)
target_link_libraries(CTAObjectStore Utils rados)
set_source_files_properties(BackendRados.cpp PROPERTIES COMPILE_FLAGS -Wno-deprecated-declarations)

set(ObjectStoreUnitTests
  BackendTest.cpp
  RootEntryTest.cpp
  TapeTest.cpp
  GarbageCollectorTest.cpp
  ArchiveToFileRequestTest.cpp
)

add_library(ctaobjectstoreunittests SHARED ${ObjectStoreUnitTests})
target_link_libraries(ctaobjectstoreunittests
  protobuf rados ctacommon gtest gmock CTAObjectStore)

add_executable(makeMinimalVFS makeMinimalVFS.cpp)
target_link_libraries(makeMinimalVFS
  protobuf CTAObjectStore ctacommon)

add_executable(listObjectStore listObjectStore.cpp)
target_link_libraries(listObjectStore
  protobuf CTAObjectStore ctacommon)
