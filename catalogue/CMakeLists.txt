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

find_package (sqlite REQUIRED)

set (CATALOGUE_LIB_SRC_FILES
  Catalogue.cpp
  ColumnNameToIdx.cpp
  DbLogin.cpp
  DummyCatalogue.cpp
  Sqlite.cpp
  SqliteCatalogue.cpp
  SqliteConn.cpp
  SqliteStmt.cpp)

add_library (ctacatalogue
  ${CATALOGUE_LIB_SRC_FILES})

set (CATALOGUE_UNIT_TESTS_LIB_SRC_FILES
  SqliteCatalogueTest.cpp
  SqliteStmtTest.cpp)

add_library (ctacatalogueunittests SHARED
  ${CATALOGUE_UNIT_TESTS_LIB_SRC_FILES})

target_link_libraries (ctacatalogueunittests
  ctacatalogue
  ${SQLITE_LIBRARIES})

install(TARGETS ctacatalogueunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
