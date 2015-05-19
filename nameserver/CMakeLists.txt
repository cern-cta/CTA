cmake_minimum_required (VERSION 2.6)

add_library (ctanameserver SHARED
  NameServer.cpp)

add_library (ctamocknameserver SHARED
  MockNameServer.cpp)

add_library (ctamocknameservertest SHARED
  MockNameServerTest.cpp)

add_library (ctanamespacevfs SHARED
  Vfs.cpp)

add_library (ctanamespacevfsunittests SHARED
  VfsTest.cpp)