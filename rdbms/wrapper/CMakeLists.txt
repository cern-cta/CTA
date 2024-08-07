# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2015-2022 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.
cmake_minimum_required (VERSION 3.17)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wshadow")

find_package (sqlite REQUIRED)
find_package (postgres REQUIRED)
include_directories (${POSTGRES_INCLUDE_DIRS})

if (OCCI_SUPPORT)
  find_package (oracle-instantclient REQUIRED)
  include_directories (${ORACLE-INSTANTCLIENT_INCLUDE_DIRS})
  add_definitions( -DSUPPORT_OCCI )
endif()

set (RDBMS_WRAPPER_LIB_SRC_FILES
  ColumnNameToIdx.cpp
  ColumnNameToIdxAndType.cpp
  ConnFactoryFactory.cpp
  ConnWrapper.cpp
  ParamNameToIdx.cpp
  RsetWrapper.cpp
  StmtWrapper.cpp
)

set (RDBMS_SQLITE_LIB_SRC_FILES
  Sqlite.cpp
  SqliteConn.cpp
  SqliteConnFactory.cpp
  SqliteRset.cpp
  SqliteStmt.cpp
)

set (RDBMS_POSTGRES_LIB_SRC_FILES
  PostgresConn.cpp
  PostgresConnFactory.cpp
  PostgresRset.cpp
  PostgresStmt.cpp
  PostgresColumn.cpp)

if (OCCI_SUPPORT)
  set (RDBMS_OCCI_LIB_SRC_FILES
    OcciColumn.cpp
    OcciConn.cpp
    OcciConnFactory.cpp
    OcciEnv.cpp
    OcciEnvSingleton.cpp
    OcciRset.cpp
    OcciStmt.cpp)
endif()

# SQLITE
## For a plugin that may not be linked by other targets,
## but may be dynamically loaded at runtime using dlopen-like functionality:
##   add_library (ctardbmssqlite MODULE
##
add_library (ctardbmssqlite SHARED
  ${RDBMS_SQLITE_LIB_SRC_FILES})
set_property(TARGET ctardbmssqlite PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctardbmssqlite PROPERTY   VERSION "${CTA_LIBVERSION}")

target_compile_definitions(ctardbmssqlite PRIVATE VERSION_API="${CTA_VERSION}-${CTA_RELEASE}")

target_link_libraries (ctardbmssqlite
  ${SQLITE_LIBRARIES})

install(TARGETS ctardbmssqlite DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

# POSTGRES
add_library (ctardbmspostgres SHARED
  ${RDBMS_POSTGRES_LIB_SRC_FILES})
set_property(TARGET ctardbmspostgres PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctardbmspostgres PROPERTY   VERSION "${CTA_LIBVERSION}")

target_compile_definitions(ctardbmspostgres PRIVATE VERSION_API="${CTA_VERSION}-${CTA_RELEASE}")

target_link_libraries (ctardbmspostgres
  ${POSTGRES_LIBRARIES})

install(TARGETS ctardbmspostgres DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

# OCCI
if (OCCI_SUPPORT) 
  add_library (ctardbmsocci SHARED
    ${RDBMS_OCCI_LIB_SRC_FILES})
  set_property(TARGET ctardbmsocci PROPERTY SOVERSION "${CTA_SOVERSION}")
  set_property(TARGET ctardbmsocci PROPERTY   VERSION "${CTA_LIBVERSION}")

  target_compile_definitions(ctardbmsocci PRIVATE VERSION_API="${CTA_VERSION}-${CTA_RELEASE}")

  target_link_libraries (ctardbmsocci
    ${ORACLE-INSTANTCLIENT_LIBRARIES})

  install(TARGETS ctardbmsocci DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
endif()

add_library (ctardbmswrapper SHARED
  ${RDBMS_WRAPPER_LIB_SRC_FILES})
set_property(TARGET ctardbmswrapper PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctardbmswrapper PROPERTY   VERSION "${CTA_LIBVERSION}")

target_compile_definitions(ctardbmswrapper PRIVATE VERSION_API="${CTA_VERSION}-${CTA_RELEASE}")

target_link_libraries (ctardbmswrapper
  ctacommon
)

if (OCCI_SUPPORT)
  set_property (TARGET ctardbmswrapper APPEND PROPERTY INSTALL_RPATH ${ORACLE-INSTANTCLIENT_RPATH})
endif()

install (TARGETS ctardbmswrapper DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

set(RDBMS_WRAPPER_UNIT_TESTS_LIB_SRC_FILES
  ConnTest.cpp
  ParamNameToIdxTest.cpp
  PostgresStmtTest.cpp
  SqliteStmtTest.cpp)

if (OCCI_SUPPORT)
  set(RDBMS_WRAPPER_UNIT_TESTS_LIB_SRC_FILES
    ${RDBMS_WRAPPER_UNIT_TESTS_LIB_SRC_FILES}
    OcciColumnTest.cpp
  )
endif()

add_library (ctardbmswrapperunittests SHARED
  ${RDBMS_WRAPPER_UNIT_TESTS_LIB_SRC_FILES})
set_property(TARGET ctardbmswrapperunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctardbmswrapperunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries (ctardbmswrapperunittests
  ctacatalogue)

install(TARGETS ctardbmswrapperunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
