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
find_package (oracle-instantclient REQUIRED)
include_directories (${POSTGRES_INCLUDE_DIRS})
include_directories (${ORACLE-INSTANTCLIENT_INCLUDE_DIRS})

set (RDBMS_WRAPPER_LIB_SRC_FILES
  ColumnNameToIdx.cpp
  ColumnNameToIdxAndType.cpp
        ConnWrapper.cpp
  ConnFactory.cpp
        RsetWrapper.cpp
        StmtWrapper.cpp
  ParamNameToIdx.cpp
  Sqlite.cpp
  SqliteConn.cpp
  SqliteConnFactory.cpp
  SqliteRset.cpp
  SqliteStmt.cpp)

set (RDBMS_WRAPPER_LIB_SRC_FILES
  ${RDBMS_WRAPPER_LIB_SRC_FILES}
  PostgresConn.cpp
  PostgresConnFactory.cpp
  PostgresRset.cpp
  PostgresStmt.cpp
  PostgresColumn.cpp)

set (RDBMS_WRAPPER_LIB_SRC_FILES
  ${RDBMS_WRAPPER_LIB_SRC_FILES}
  ConnFactoryFactory.cpp
  OcciColumn.cpp
  OcciConn.cpp
  OcciConnFactory.cpp
  OcciEnv.cpp
  OcciEnvSingleton.cpp
  OcciRset.cpp
  OcciStmt.cpp)

add_library (ctardbmswrapper SHARED
  ${RDBMS_WRAPPER_LIB_SRC_FILES})
set_property(TARGET ctardbmswrapper PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctardbmswrapper PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries (ctardbmswrapper
  ctacommon
  ${SQLITE_LIBRARIES}
  ${POSTGRES_LIBRARIES}
  ${ORACLE-INSTANTCLIENT_LIBRARIES})

install (TARGETS ctardbmswrapper DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

set(RDBMS_WRAPPER_UNIT_TESTS_LIB_SRC_FILES
  ConnTest.cpp
  OcciColumnTest.cpp
  ParamNameToIdxTest.cpp
  PostgresStmtTest.cpp
  SqliteStmtTest.cpp)

add_library (ctardbmswrapperunittests SHARED
  ${RDBMS_WRAPPER_UNIT_TESTS_LIB_SRC_FILES})
set_property(TARGET ctardbmswrapperunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctardbmswrapperunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries (ctardbmswrapperunittests
  ctacatalogue)

install(TARGETS ctardbmswrapperunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
