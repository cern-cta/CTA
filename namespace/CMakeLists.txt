cmake_minimum_required (VERSION 2.6)

add_library (ctanamespacevfs SHARED
  Vfs.cpp)

add_library (ctanamespacevfsunittests SHARED
  VfsTest.cpp)