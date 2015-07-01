cmake_minimum_required (VERSION 2.6)

add_library (ctanameserver SHARED
  MockNameServer.cpp
  CastorNameServer.cpp
  NameServer.cpp)

target_link_libraries(ctanameserver castorns castorcommon)

add_library (ctanameservertest SHARED
  MockNameServerFactory.cpp
  MockNameServerTest.cpp
  NameServerFactory.cpp)

target_link_libraries(ctanameservertest
  ctanameserver)
