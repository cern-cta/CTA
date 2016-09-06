cmake_minimum_required (VERSION 2.6)

include_directories(${PROJECT_SOURCE_DIR}/tapeserver)
include_directories(${PROJECT_SOURCE_DIR}/tapeserver/h)

#add_library (ctaserverutils
#  ProcessCapDummy.cpp
#  Semaphores.cpp
#  Mutex.cpp
#  Threading.cpp
#  ProcessCap.cpp
#  SmartCap.cpp
#  Daemon.cpp)
#
#target_link_libraries(ctaserverutils cap ctautils)

add_library (ctaserverunittests SHARED
  DaemonTest.cpp)

target_link_libraries (ctaserverunittests ctautils)

install(TARGETS ctaserverunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
