cmake_minimum_required (VERSION 2.6)

add_library (ctanameserver SHARED
  NameServer.cpp)

add_library (ctamocknameserver SHARED
  MockNameServer.cpp)

target_link_libraries(ctamocknameserver
  ctanameserver)

add_library (ctamocknameservertest SHARED
  MockNameServerTest.cpp
  NameServerFactory.cpp)

target_link_libraries(ctamocknameservertest
  ctamocknameserver)
