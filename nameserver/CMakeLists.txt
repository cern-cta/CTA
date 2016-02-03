cmake_minimum_required (VERSION 2.6)

include_directories(/usr/include/shift)

add_library (ctanameserver SHARED
  mockNS/MockNameServer.cpp
  mockNS/SmartFd.cpp
  NameServer.cpp
  NameServerTapeFile.cpp)

target_link_libraries(ctanameserver ctacommon)

install (TARGETS ctanameserver DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

add_library (ctanameserverunittests SHARED
  mockNS/MockNameServerFactory.cpp
  mockNS/MockNameServerTest.cpp
  mockNS/SmartFdTest.cpp
  NameServerFactory.cpp)

target_link_libraries(ctanameserverunittests
  ctanameserver)

add_executable(makeMockNameServerBasePath mockNS/makeMockNameServerBasePath.cpp)
target_link_libraries(makeMockNameServerBasePath ctacommon)