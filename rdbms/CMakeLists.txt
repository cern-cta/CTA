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

if(OCCI_SUPPORT)
  find_package (oracle-instantclient REQUIRED)
  include_directories (${ORACLE-INSTANTCLIENT_INCLUDE_DIRS})
endif(OCCI_SUPPORT)

set (RDBMS_LIB_SRC_FILES
  AutoRollback.cpp
  ColumnNameToIdx.cpp
  ColumnNameToIdxAndType.cpp
  Conn.cpp
  ConnFactory.cpp
  ConnPool.cpp
  Login.cpp
  LoginFactory.cpp
  Rset.cpp
  RsetImpl.cpp
  Stmt.cpp
  NullDbValue.cpp
  ParamNameToIdx.cpp
  PooledConn.cpp
  Sqlite.cpp
  SqliteConn.cpp
  SqliteConnFactory.cpp
        SqliteRsetImpl.cpp
  SqliteStmt.cpp)

if (OCCI_SUPPORT)
  set (RDBMS_LIB_SRC_FILES
    ${RDBMS_LIB_SRC_FILES}
    ConnFactoryFactory.cpp
    OcciColumn.cpp
    OcciConn.cpp
    OcciConnFactory.cpp
    OcciEnv.cpp
    OcciEnvSingleton.cpp
          OcciRsetImpl.cpp
    OcciStmt.cpp)
else (OCCI_SUPPORT)
  set (RDBMS_LIB_SRC_FILES
    ${RDBMS_LIB_SRC_FILES}
    ConnFactoryFactory_OCCI_SUPPORT_OFF.cpp)
endif (OCCI_SUPPORT)

add_library (ctardbms SHARED
  ${RDBMS_LIB_SRC_FILES})
set_property(TARGET ctardbms PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctardbms PROPERTY   VERSION "${CTA_LIBVERSION}")

set (CTARDBMS_LINK_LIBRARIES
  ctacommon
  ${SQLITE_LIBRARIES})

if (OCCI_SUPPORT)
  set (CTARDBMS_LINK_LIBRARIES
    ${CTARDBMS_LINK_LIBRARIES}
    ${ORACLE-INSTANTCLIENT_LIBRARIES})
endif (OCCI_SUPPORT)

target_link_libraries (ctardbms
  ${CTARDBMS_LINK_LIBRARIES})

install (TARGETS ctardbms DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

set(RDBMS_UNIT_TESTS_LIB_SRC_FILES
  ConnPoolTest.cpp
  LoginTest.cpp
  ParamNameToIdxTest.cpp
  SqliteStmtTest.cpp)

if (OCCI_SUPPORT)
  list (APPEND RDBMS_UNIT_TESTS_LIB_SRC_FILES OcciColumnTest.cpp)
endif (OCCI_SUPPORT)

add_library (ctardbmsunittests SHARED
  ${RDBMS_UNIT_TESTS_LIB_SRC_FILES})
set_property(TARGET ctardbmsunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctardbmsunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries (ctardbmsunittests
  ctacatalogue)

install(TARGETS ctardbmsunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
