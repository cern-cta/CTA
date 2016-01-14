cmake_minimum_required (VERSION 2.6)

include_directories(${PROJECT_SOURCE_DIR}/tapeserver)
include_directories(${PROJECT_SOURCE_DIR}/tapeserver/h)

add_library (ctaserverutils SHARED
  ProcessCapDummy.cpp
  Semaphores.cpp
  Mutex.cpp
  Threading.cpp
  ProcessCap.cpp
  Daemon.cpp)

add_library (ctaserverunittests SHARED
  AtomicCounterTest.cpp
  DaemonTest.cpp
  SmartCapTest.cpp
  ThreadingBlockingQTests.cpp
# ThreadingMPTests.cpp is commented out because of errors caused by libust
  ThreadingMTTests.cpp
  ThreadingTests.cpp)

target_link_libraries (ctaserverunittests)
