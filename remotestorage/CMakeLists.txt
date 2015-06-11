cmake_minimum_required (VERSION 2.6)

set (CTA_REMOTE_STORAGE_SRC_FILES
  EosRemoteStorage.cpp
  RemoteStorage.cpp)

add_library (ctaremotestorage SHARED
  ${CTA_REMOTE_STORAGE_SRC_FILES})

add_library (ctaremotestoragetest SHARED
  MockRemoteStorage.cpp
  MockRemoteStorageFactory.cpp
  RemoteStorageFactory.cpp)

target_link_libraries(ctaremotestoragetest
  ctaremotestorage)
