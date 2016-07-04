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

find_package (oracle-instantclient REQUIRED)
find_package (sqlite REQUIRED)

include_directories (${ORACLE-INSTANTCLIENT_INCLUDE_DIRS})

set (RDBMS_LIB_SRC_FILES
  AutoRollback.cpp
  ColumnNameToIdx.cpp
  ColumnNameToIdxAndType.cpp
  DbConn.cpp
  DbLogin.cpp
  DbRset.cpp
  DbStmt.cpp
  NullDbValue.cpp
  OcciConn.cpp
  OcciEnv.cpp
  OcciEnvSingleton.cpp
  OcciRset.cpp
  OcciStmt.cpp
  ParamNameToIdx.cpp
  Sqlite.cpp
  SqliteConn.cpp
  SqliteRset.cpp
  SqliteStmt.cpp)

add_library (ctardbms SHARED
  ${RDBMS_LIB_SRC_FILES})

target_link_libraries (ctardbms
  ctacommon
  ${ORACLE-INSTANTCLIENT_LIBRARIES}
  ${SQLITE_LIBRARIES})

install (TARGETS ctardbms DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
