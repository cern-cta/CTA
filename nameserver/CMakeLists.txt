cmake_minimum_required (VERSION 2.6)

add_library (ctanameserver SHARED
  NameServer.cpp)

add_library (ctanameservertest SHARED
  MockNameServer.cpp
  MockNameServerFactory.cpp
  MockNameServerTest.cpp
  NameServerFactory.cpp)

target_link_libraries(ctanameservertest
  ctanameserver)
