cmake_minimum_required (VERSION 2.6)

include_directories(${PROJECT_SOURCE_DIR}/tapeserver)
include_directories(${PROJECT_SOURCE_DIR}/tapeserver/h)

add_library (ctaserverutils
  ProcessCapDummy.cpp
  Semaphores.cpp
  Mutex.cpp
  Threading.cpp
  ProcessCap.cpp
  SmartCap.cpp
  Daemon.cpp)

target_link_libraries(ctaserverutils cap ctautils)

add_library (ctaserverunittests SHARED
  AtomicCounterTest.cpp
  DaemonTest.cpp
  SmartCapTest.cpp
  ThreadingBlockingQTests.cpp
# ThreadingMPTests.cpp is commented out because of errors caused by libust
  ThreadingMTTests.cpp
  ThreadingTests.cpp)

target_link_libraries (ctaserverunittests)
