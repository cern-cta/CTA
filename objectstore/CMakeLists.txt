# The CERN Tape Archive(CTA) project
# Copyright(C) 2015  CERN
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

cmake_minimum_required (VERSION 2.6)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../middletier)


find_package(json-c REQUIRED)
find_package(librados2 REQUIRED)
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
  objectstore/TapeQueue.hpp
  objectstore/ArchiveQueue.hpp
  objectstore/UserIdentity.hpp
  objectstore/ValueCountMap.hpp)

SET_SOURCE_FILES_PROPERTIES(${CTAProtoHeaders} PROPERTIES HEADER_FILE_ONLY TRUE)
SET_SOURCE_FILES_PROPERTIES(${CTAProtoDependants} 
  PROPERTIES OBJECT_DEPENDS ${CTAProtoHeaders})


add_library (ctaobjectstore SHARED
  ${CTAProtoSources}
  RootEntry.cpp
  Agent.cpp
  AgentRegister.cpp
  AgentWatchdog.cpp
  ArchiveQueue.cpp
  RetrieveQueue.cpp
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
  SchedulerGlobalLock.cpp
  ValueCountMap.cpp)
target_link_libraries(ctaobjectstore Utils rados json-c ctautils)
set_source_files_properties(BackendRados.cpp PROPERTIES COMPILE_FLAGS -Wno-deprecated-declarations)
install (TARGETS ctaobjectstore DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

set(ObjectStoreUnitTests
  BackendTest.cpp
  RootEntryTest.cpp
  RetrieveQueueTest.cpp
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
