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

if (ALMA9)
  add_definitions( -DALMA9 )
endif()

add_subdirectory (wrapper)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wshadow")

set (RDBMS_LIB_SRC_FILES
  AutoRollback.cpp
  CheckConstraintError.cpp
  Conn.cpp
  ConnPool.cpp
  DBException.cpp
  ConstraintError.cpp
  Login.cpp
  LoginFactory.cpp
  NullDbValue.cpp
  rdbms.cpp
  Rset.cpp
  Stmt.cpp
  StmtPool.cpp
  UniqueConstraintError.cpp)

add_library (ctardbms SHARED
  ${RDBMS_LIB_SRC_FILES})
set_property(TARGET ctardbms PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctardbms PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries (ctardbms
  ctardbmswrapper)

install (TARGETS ctardbms DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

set(RDBMS_UNIT_TESTS_LIB_SRC_FILES
  ConnPoolTest.cpp
  LoginTest.cpp
  RdbmsTest.cpp
  RsetTest.cpp
  StmtPoolTest.cpp)

add_library (ctardbmsunittests SHARED
  ${RDBMS_UNIT_TESTS_LIB_SRC_FILES})
set_property(TARGET ctardbmsunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctardbmsunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries (ctardbmsunittests
  ctardbms)

install(TARGETS ctardbmsunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

set(IN_MEMORY_CONN_UNIT_TESTS_LIB_SRC_FILES
  ConnTest.cpp
  InMemoryVersionOfConnTest.cpp)

add_library (ctainmemoryconnunittests SHARED
  ${IN_MEMORY_CONN_UNIT_TESTS_LIB_SRC_FILES})
set_property(TARGET ctainmemoryconnunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctainmemoryconnunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries (ctainmemoryconnunittests
  ctardbms)

install (TARGETS ctainmemoryconnunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

set(IN_MEMORY_STMT_UNIT_TESTS_LIB_SRC_FILES
  StmtTest.cpp
  InMemoryVersionOfStmtTest.cpp)

add_library (ctainmemorystmtunittests SHARED
  ${IN_MEMORY_STMT_UNIT_TESTS_LIB_SRC_FILES})
set_property(TARGET ctainmemorystmtunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctainmemorystmtunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries (ctainmemorystmtunittests
  ctardbms)

install (TARGETS ctainmemorystmtunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

set (DBCONFIG_CONN_UNIT_TESTS_LIB_SRC_FILES
  ConnTest.cpp
  DbConfigVersionOfConnTest.cpp)

add_library (ctadbconfigconnunittests SHARED
  ${DBCONFIG_CONN_UNIT_TESTS_LIB_SRC_FILES})
set_property(TARGET ctadbconfigconnunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctadbconfigconnunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries (ctadbconfigconnunittests
  ctardbms)

install (TARGETS ctadbconfigconnunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

set (DBCONFIG_STMT_UNIT_TESTS_LIB_SRC_FILES
  StmtTest.cpp
  DbConfigVersionOfStmtTest.cpp)

add_library (ctadbconfigstmtunittests SHARED
  ${DBCONFIG_STMT_UNIT_TESTS_LIB_SRC_FILES})
set_property(TARGET ctadbconfigstmtunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctadbconfigstmtunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries (ctadbconfigstmtunittests
  ctardbms)

install (TARGETS ctadbconfigstmtunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
