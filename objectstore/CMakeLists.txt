cmake_minimum_required (VERSION 2.6)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../middletier)


find_package(json-c REQUIRED)
find_package(Protobuf REQUIRED)

set (CTAProtoFiles
  cta.proto)

PROTOBUF_GENERATE_CPP(CTAProtoSources CTAProtoHeaders ${CTAProtoFiles})

set (CTAProtoDependants objectstore/Agent.hpp
  objectstore/ArchiveToFileRequest.hpp
  objectstore/ArchiveRequest.hpp
  objectstore/CreationLog.hpp
  objectstore/GenericObject.hpp
  objectstore/ObjectOps.hpp
  objectstore/RetrieveRequest.hpp
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


add_library (ctaobjectstore SHARED
  ${CTAProtoSources}
  RootEntry.cpp
  Agent.cpp
  AgentRegister.cpp
  AgentWatchdog.cpp
  TapePool.cpp
  Tape.cpp
  ArchiveToFileRequest.cpp
  ArchiveRequest.cpp
  RetrieveToFileRequest.cpp
  RetrieveRequest.cpp
  DriveRegister.cpp
  BackendVFS.cpp
  BackendRados.cpp
  BackendPopulator.cpp
  BackendFactory.cpp
  ProtocolBuffersAlgorithms.cpp
  GenericObject.cpp
  GarbageCollector.cpp
  SchedulerGlobalLock.cpp)
target_link_libraries(ctaobjectstore Utils rados json-c ctautils)
set_source_files_properties(BackendRados.cpp PROPERTIES COMPILE_FLAGS -Wno-deprecated-declarations)
install (TARGETS ctaobjectstore DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

set(ObjectStoreUnitTests
  BackendTest.cpp
  RootEntryTest.cpp
  TapeTest.cpp
  GarbageCollectorTest.cpp
  ArchiveToFileRequestTest.cpp
)

add_library(ctaobjectstoreunittests SHARED ${ObjectStoreUnitTests})
target_link_libraries(ctaobjectstoreunittests
  protobuf rados ctacommon gtest ctaobjectstore)
install(TARGETS ctaobjectstoreunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

add_executable(makeMinimalVFS makeMinimalVFS.cpp)
target_link_libraries(makeMinimalVFS
  protobuf ctaobjectstore ctacommon ctautils)

add_executable(listObjectStore listObjectStore.cpp)
target_link_libraries(listObjectStore
  protobuf ctaobjectstore ctacommon)

add_executable(dumpObject dumpObject.cpp)
target_link_libraries(dumpObject
  protobuf ctaobjectstore ctacommon)
