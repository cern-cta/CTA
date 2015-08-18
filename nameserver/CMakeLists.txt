cmake_minimum_required (VERSION 2.6)

include_directories(/usr/include/shift)

add_library (ctanameserver SHARED
  mockNS/MockNameServer.cpp
  mockNS/SmartFd.cpp
  CastorNameServer.cpp
  NameServer.cpp)

target_link_libraries(ctanameserver castorns castorcommon)

add_executable(castornstest CastorNameServerTest.cpp)

target_link_libraries(castornstest ctanameserver ctacommon ctascheduler)

add_library (ctanameserverunittests SHARED
  mockNS/MockNameServerFactory.cpp
  mockNS/MockNameServerTest.cpp
  mockNS/SmartFdTest.cpp
  NameServerFactory.cpp)

target_link_libraries(ctanameserverunittests
  ctanameserver)
