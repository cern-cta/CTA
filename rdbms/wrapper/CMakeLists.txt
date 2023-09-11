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
  ConnFactory.cpp
  ConnFactoryFactory.cpp
  ConnWrapper.cpp
  ParamNameToIdx.cpp
  RsetWrapper.cpp
  Sqlite.cpp
  SqliteConn.cpp
  SqliteConnFactory.cpp
  SqliteRset.cpp
  SqliteStmt.cpp
  StmtWrapper.cpp
)

set (RDBMS_WRAPPER_LIB_SRC_FILES
  ${RDBMS_WRAPPER_LIB_SRC_FILES}
  PostgresConn.cpp
  PostgresConnFactory.cpp
  PostgresRset.cpp
  PostgresStmt.cpp
  PostgresColumn.cpp)

if (OCCI_SUPPORT)
  set (RDBMS_WRAPPER_LIB_SRC_FILES
    ${RDBMS_WRAPPER_LIB_SRC_FILES}
    OcciColumn.cpp
    OcciConn.cpp
    OcciConnFactory.cpp
    OcciEnv.cpp
    OcciEnvSingleton.cpp
    OcciRset.cpp
    OcciStmt.cpp)
endif()

add_library (ctardbmswrapper SHARED
  ${RDBMS_WRAPPER_LIB_SRC_FILES})
set_property(TARGET ctardbmswrapper PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctardbmswrapper PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries (ctardbmswrapper
  ctacommon
  ${SQLITE_LIBRARIES}
  ${POSTGRES_LIBRARIES})

if (OCCI_SUPPORT)
  target_link_libraries (ctardbmswrapper ${ORACLE-INSTANTCLIENT_LIBRARIES})
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
