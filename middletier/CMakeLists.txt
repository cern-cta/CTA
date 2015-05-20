cmake_minimum_required (VERSION 2.6)

find_package (sqlite REQUIRED)
include_directories (${SQLITE3_INCLUDE_DIR} ${CMAKE_CURRENT_SOURCE_DIR} 
  ${PROJECT_SOURCE_DIR})

set (MIDDLE_TIER_COMMON_UNIT_TESTS_LIB_SRC_FILES
  sharedtest/MiddleTierAdminAbstractTest.cpp
  sharedtest/MiddleTierUserAbstractTest.cpp)

add_library (ctamiddletierunittests SHARED
  ${MIDDLE_TIER_COMMON_UNIT_TESTS_LIB_SRC_FILES})

add_library (ctamiddletiersqlite SHARED
  SQLite/SqliteColumnNameToIndex.cpp
  SQLite/SqliteDatabase.cpp
  SQLite/SqliteMiddleTierAdmin.cpp
  SQLite/SqliteMiddleTierUser.cpp)

target_link_libraries (ctamiddletiersqlite ${SQLITE3_LIBRARY_RELEASE})

add_library (ctamiddletiersqliteunittests SHARED
  SQLite/SqliteMiddleTierTest.cpp)

include_directories(${CMAKE_BINARY_DIR})

add_library(ctamiddletierobjectstore
  objectstore/ObjectStoreMiddleTierAdmin.cpp
  objectstore/ObjectStoreMiddleTierUser.cpp)

add_library (ctamiddletierobjectstoreunittests SHARED
  objectstore/ObjectStoreMiddleTierTest.cpp)
