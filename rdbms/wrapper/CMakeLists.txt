# The CERN Tape Archive (CTA) project
# Copyright (C) 2015  CERN
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
cmake_minimum_required (VERSION 2.6)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wshadow")

find_package (sqlite REQUIRED)

find_package (oracle-instantclient REQUIRED)
include_directories (${ORACLE-INSTANTCLIENT_INCLUDE_DIRS})

set (RDBMS_WRAPPER_LIB_SRC_FILES
  ColumnNameToIdx.cpp
  ColumnNameToIdxAndType.cpp
  Conn.cpp
  ConnFactory.cpp
  Rset.cpp
  Stmt.cpp
  ParamNameToIdx.cpp
  Sqlite.cpp
  SqliteConn.cpp
  SqliteConnFactory.cpp
  SqliteRset.cpp
  SqliteStmt.cpp)

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
  ${ORACLE-INSTANTCLIENT_LIBRARIES})

install (TARGETS ctardbmswrapper DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

set(RDBMS_UNIT_TESTS_LIB_SRC_FILES
  ConnTest.cpp
  OcciColumnTest.cpp
  ParamNameToIdxTest.cpp
  SqliteStmtTest.cpp)

add_library (ctardbmswrapperunittests SHARED
  ${RDBMS_UNIT_TESTS_LIB_SRC_FILES})
set_property(TARGET ctardbmswrapperunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctardbmswrapperunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries (ctardbmswrapperunittests
  ctacatalogue)

install(TARGETS ctardbmswrapperunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
