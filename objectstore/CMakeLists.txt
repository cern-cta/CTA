cmake_minimum_required (VERSION 2.6)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_BINARY_DIR})

find_package(Protobuf)

set (CTAProtoFiles
  cta.proto)

PROTOBUF_GENERATE_CPP(CTAProtoSources CTAProtoHeaders ${CTAProtoFiles})

add_executable (tapeResourceMangerTest tapeResourceManagerTest.cpp
  ${CTAProtoSources}
  RootEntry.cpp
  Agent.cpp
  AgentRegister.cpp
  exception/Backtrace.cpp
  exception/Errnum.cpp
  exception/Exception.cpp
  exception/strerror_r_wrapper.cpp
  threading/ChildProcess.cpp
  threading/Mutex.cpp
  threading/Threading.cpp
  utils/Timer.cpp)

target_link_libraries(tapeResourceMangerTest
  protobuf
  rados)
