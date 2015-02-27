cmake_minimum_required (VERSION 2.6)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_BINARY_DIR})

find_package(Protobuf)

set (CTAProtoFiles
  cta.proto)

PROTOBUF_GENERATE_CPP(CTAProtoSources CTAProtoHeaders ${CTAProtoFiles})

add_library (CTAObjectStore 
  ${CTAProtoSources}
  RootEntry.cpp
  Agent.cpp
  AgentRegister.cpp
  AgentWatchdog.cpp
  BackendVFS.cpp
  BackendRados.cpp
  ObjectOps.cpp
  ProtocolBuffersAlgorithms.cpp
  FIFO.cpp
  GenericObject.cpp
  GarbageCollector.cpp
  exception/Backtrace.cpp
  exception/Errnum.cpp
  exception/Exception.cpp
  exception/strerror_r_wrapper.cpp
  threading/ChildProcess.cpp
  threading/Mutex.cpp
  threading/Threading.cpp
  utils/Timer.cpp)

# add_executable(tapeResourceManagerTest tapeResourceManagerTest.cpp)
# target_link_libraries(tapeResourceManagerTest
#   protobuf rados CTAObjectStore)
# 
# add_executable(dumpStructure dumpStructure.cpp)
# target_link_libraries(dumpStructure
#   protobuf rados CTAObjectStore)
# 
# add_executable(jobPoster jobPoster.cpp)
# target_link_libraries(jobPoster
#   protobuf rados CTAObjectStore)
# 
# add_executable(recaller recaller.cpp)
# target_link_libraries(recaller
#   protobuf rados CTAObjectStore)
# 
# add_executable(garbageCollector garbageCollector.cpp)
# target_link_libraries(garbageCollector
#   protobuf rados CTAObjectStore)
# 
# add_executable(createEnvironment createEnvironment.cpp)
# target_link_libraries(createEnvironment
#   protobuf rados CTAObjectStore)

set(ObjectStoreUnitTests
  BackendTest.cpp
  RootEntryTest.cpp
  FIFOTest.cpp
  GarbageCollectorTest.cpp
)

add_executable(unitTests unitTests.cpp ${ObjectStoreUnitTests})
target_link_libraries(unitTests
  protobuf rados CTAObjectStore gtest gmock)