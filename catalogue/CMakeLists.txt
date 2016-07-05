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

set (CATALOGUE_LIB_SRC_FILES
  ArchiveFileRow.cpp
  ArchiveFileItor.cpp
  Catalogue.cpp
  CatalogueFactory.cpp
  InMemoryCatalogue.cpp
  InMemoryCatalogueSchema.cpp
  TapeFileWritten.cpp
  OracleCatalogue.cpp
  RdbmsCatalogue.cpp
  SqliteCatalogue.cpp
  TapeForWriting.cpp)

add_library (ctacatalogue SHARED
  ${CATALOGUE_LIB_SRC_FILES})

install (TARGETS ctacatalogue DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

target_link_libraries (ctacatalogue
  ctacommon
  ctardbms
  ${ORACLE-INSTANTCLIENT_LIBRARIES}
  ${SQLITE_LIBRARIES})

add_custom_command (OUTPUT create_sqlite_catalogue_schema.sql create_oracle_catalogue_schema.sql
  COMMAND cat ${CMAKE_CURRENT_SOURCE_DIR}/create_sqlite_catalogue_schema_header.sql ${CMAKE_CURRENT_SOURCE_DIR}/catalogue_common_schema.sql ${CMAKE_CURRENT_SOURCE_DIR}/create_sqlite_catalogue_schema_trailer.sql > create_sqlite_catalogue_schema.sql
  COMMAND cat ${CMAKE_CURRENT_SOURCE_DIR}/create_oracle_catalogue_schema_header.sql ${CMAKE_CURRENT_SOURCE_DIR}/catalogue_common_schema.sql ${CMAKE_CURRENT_SOURCE_DIR}/create_oracle_catalogue_schema_trailer.sql > create_oracle_catalogue_schema.sql
  DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/catalogue_common_schema.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/create_sqlite_catalogue_schema_header.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/create_sqlite_catalogue_schema_trailer.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/create_oracle_catalogue_schema_header.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/create_oracle_catalogue_schema_trailer.sql)

install (FILES ${CMAKE_CURRENT_BINARY_DIR}/create_sqlite_catalogue_schema.sql
  DESTINATION usr/share/cta-${CTA_VERSION}/sql
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

install (FILES ${CMAKE_CURRENT_BINARY_DIR}/create_oracle_catalogue_schema.sql
  DESTINATION usr/share/cta-${CTA_VERSION}/sql
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

add_custom_command(OUTPUT in_memory_catalogue_schema.cpp
  COMMAND sed 's/^/\ \ \"/' create_sqlite_catalogue_schema.sql | sed 's/$$/\"/' > in_memory_catalogue_schema.cpp
  DEPENDS create_sqlite_catalogue_schema.sql)

add_custom_command(OUTPUT InMemoryCatalogueSchema.cpp
  COMMAND sed '/CTA_SQL_SCHEMA/r in_memory_catalogue_schema.cpp' ${CMAKE_CURRENT_SOURCE_DIR}/InMemoryCatalogueSchema.before_SQL.cpp > InMemoryCatalogueSchema.cpp
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/InMemoryCatalogueSchema.before_SQL.cpp in_memory_catalogue_schema.cpp)

set(IN_MEMORY_CATALOGUE_UNIT_TESTS_LIB_SRC_FILES
  CatalogueTest.cpp
  CatalogueFactoryTest.cpp
  ../rdbms/DbLoginFactory.cpp
  ../rdbms/DbLoginTest.cpp
  ../rdbms/ParamNameToIdxTest.cpp
  InMemoryCatalogueTest.cpp
  ../rdbms/SqliteStmtTest.cpp)

add_library (ctainmemorycatalogueunittests SHARED
  ${IN_MEMORY_CATALOGUE_UNIT_TESTS_LIB_SRC_FILES})

target_link_libraries (ctainmemorycatalogueunittests
  ctacatalogue)

install(TARGETS ctainmemorycatalogueunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

set (CATALOGUE_UNIT_TESTS_LIB_SRC_FILES
  CatalogueTest.cpp
  DbConfigCatalogueTest.cpp
  ../rdbms/DbLoginFactory.cpp)

add_library (ctacatalogueunittests SHARED
  ${CATALOGUE_UNIT_TESTS_LIB_SRC_FILES})

target_link_libraries (ctacatalogueunittests
  ctacatalogue)

install(TARGETS ctacatalogueunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

install (FILES cta_catalogue_db.conf.example
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)
