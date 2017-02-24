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

# OCCI support is on by default
set (OCCI_SUPPORT ON)

# Switch OCCI support off if using gcc 5.1 or higher
if (CMAKE_COMPILER_IS_GNUCC)
  if (GCC_VERSION VERSION_GREATER 5.0)
    message(STATUS "Detected gcc >= 5.1 - Switching off OCCI support")
    set (OCCI_SUPPORT OFF)
  endif (GCC_VERSION VERSION_GREATER 5.0)
endif (CMAKE_COMPILER_IS_GNUCC)

if(OCCI_SUPPORT)
  find_package (oracle-instantclient REQUIRED)
  include_directories (${ORACLE-INSTANTCLIENT_INCLUDE_DIRS})
endif(OCCI_SUPPORT)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wshadow")

set (CATALOGUE_LIB_SRC_FILES
  ArchiveFileRow.cpp
  ArchiveFileItor.cpp
  Catalogue.cpp
  CmdLineTool.cpp
  InMemoryCatalogue.cpp
  SqliteCatalogueSchema.cpp
  TapeFileWritten.cpp
  RdbmsCatalogue.cpp
  SchemaCreatingSqliteCatalogue.cpp
  SqliteCatalogue.cpp
  TapeForWriting.cpp)

if (OCCI_SUPPORT)
  set (CATALOGUE_LIB_SRC_FILES
    ${CATALOGUE_LIB_SRC_FILES}
    CatalogueFactory.cpp
    OracleCatalogue.cpp)
else (OCCI_SUPPORT)
  set (CATALOGUE_LIB_SRC_FILES
    ${CATALOGUE_LIB_SRC_FILES}
    CatalogueFactory_OCCI_SUPPORT_OFF.cpp)
endif (OCCI_SUPPORT)

add_library (ctacatalogue SHARED
  ${CATALOGUE_LIB_SRC_FILES})

install (TARGETS ctacatalogue DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

target_link_libraries (ctacatalogue
  ctacommon
  ctardbms)

add_custom_command (OUTPUT sqlite_catalogue_schema.sql oracle_catalogue_schema.sql
  COMMAND cat ${CMAKE_CURRENT_SOURCE_DIR}/sqlite_catalogue_schema_header.sql ${CMAKE_CURRENT_SOURCE_DIR}/catalogue_common_schema.sql ${CMAKE_CURRENT_SOURCE_DIR}/sqlite_catalogue_schema_trailer.sql > sqlite_catalogue_schema.sql
  COMMAND cat ${CMAKE_CURRENT_SOURCE_DIR}/oracle_catalogue_schema_header.sql ${CMAKE_CURRENT_SOURCE_DIR}/catalogue_common_schema.sql ${CMAKE_CURRENT_SOURCE_DIR}/oracle_catalogue_schema_trailer.sql > oracle_catalogue_schema.sql
  DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/catalogue_common_schema.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/sqlite_catalogue_schema_header.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/sqlite_catalogue_schema_trailer.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/oracle_catalogue_schema_header.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/oracle_catalogue_schema_trailer.sql)

add_custom_command(OUTPUT sqlite_catalogue_schema.cpp
  COMMAND sed 's/^/\ \ \"/' sqlite_catalogue_schema.sql | sed 's/$$/\"/' > sqlite_catalogue_schema.cpp
  DEPENDS sqlite_catalogue_schema.sql)

add_custom_command(OUTPUT SqliteCatalogueSchema.cpp
  COMMAND sed '/CTA_SQL_SCHEMA/r sqlite_catalogue_schema.cpp' ${CMAKE_CURRENT_SOURCE_DIR}/SqliteCatalogueSchema.before_SQL.cpp > SqliteCatalogueSchema.cpp
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/SqliteCatalogueSchema.before_SQL.cpp sqlite_catalogue_schema.cpp)

set(IN_MEMORY_CATALOGUE_UNIT_TESTS_LIB_SRC_FILES
  CatalogueTest.cpp
  CatalogueFactoryTest.cpp
  InMemoryCatalogueTest.cpp)

add_library (ctainmemorycatalogueunittests SHARED
  ${IN_MEMORY_CATALOGUE_UNIT_TESTS_LIB_SRC_FILES})

target_link_libraries (ctainmemorycatalogueunittests
  ctacatalogue)

install (TARGETS ctainmemorycatalogueunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

set (CATALOGUE_UNIT_TESTS_LIB_SRC_FILES
  CatalogueTest.cpp
  DbConfigCatalogueTest.cpp)

add_library (ctacatalogueunittests SHARED
  ${CATALOGUE_UNIT_TESTS_LIB_SRC_FILES})

target_link_libraries (ctacatalogueunittests
  ctacatalogue)

install (TARGETS ctacatalogueunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

install (FILES cta_catalogue_db.conf.example
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

add_executable(cta-catalogue-schema-lock
  LockSchemaCmd.cpp
  LockSchemaCmdLineArgs.cpp
  LockSchemaCmdMain.cpp)

target_link_libraries (cta-catalogue-schema-lock
  ctacatalogue)

install (TARGETS cta-catalogue-schema-lock DESTINATION /usr/bin)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-catalogue-schema-lock.1cta DESTINATION /usr/share/man/man1)

add_executable(cta-catalogue-schema-unlock
  UnlockSchemaCmd.cpp
  UnlockSchemaCmdLineArgs.cpp
  UnlockSchemaCmdMain.cpp)

target_link_libraries (cta-catalogue-schema-unlock
  ctacatalogue)

install (TARGETS cta-catalogue-schema-unlock DESTINATION /usr/bin)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-catalogue-schema-unlock.1cta DESTINATION /usr/share/man/man1)

add_executable(cta-catalogue-schema-status
  SchemaStatusCmd.cpp
  SchemaStatusCmdLineArgs.cpp
  SchemaStatusCmdMain.cpp)

target_link_libraries (cta-catalogue-schema-status
  ctacatalogue)

install (TARGETS cta-catalogue-schema-status DESTINATION /usr/bin)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-catalogue-schema-status.1cta DESTINATION /usr/share/man/man1)

add_executable(cta-catalogue-delete-all-data
  DeleteAllCatalogueDataCmd.cpp
  DeleteAllCatalogueDataCmdLineArgs.cpp
  DeleteAllCatalogueDataCmdMain.cpp)

target_link_libraries (cta-catalogue-delete-all-data
  ctacatalogue)

install (TARGETS cta-catalogue-delete-all-data DESTINATION /usr/bin)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-catalogue-delete-all-data.1cta DESTINATION /usr/share/man/man1)

add_custom_command(OUTPUT oracle_catalogue_schema.cpp
  COMMAND sed 's/^/\ \ \"/' oracle_catalogue_schema.sql | sed 's/$$/\"/' > oracle_catalogue_schema.cpp
  DEPENDS oracle_catalogue_schema.sql)

add_custom_command(OUTPUT OracleCatalogueSchema.cpp
  COMMAND sed '/CTA_SQL_SCHEMA/r oracle_catalogue_schema.cpp' ${CMAKE_CURRENT_SOURCE_DIR}/OracleCatalogueSchema.before_SQL.cpp > OracleCatalogueSchema.cpp
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/OracleCatalogueSchema.before_SQL.cpp oracle_catalogue_schema.cpp)

add_executable(cta-catalogue-schema-create
  CreateSchemaCmd.cpp
  CreateSchemaCmdLineArgs.cpp
  CreateSchemaCmdMain.cpp
  OracleCatalogueSchema.cpp
  SqliteCatalogueSchema.cpp)

target_link_libraries (cta-catalogue-schema-create
  ctacatalogue)

add_custom_command(OUTPUT drop_oracle_catalogue_schema.cpp
  COMMAND sed 's/^/\ \ \"/' ${CMAKE_CURRENT_SOURCE_DIR}/drop_oracle_catalogue_schema.sql | sed 's/$$/\"/' > drop_oracle_catalogue_schema.cpp
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/drop_oracle_catalogue_schema.sql)

add_custom_command(OUTPUT DropOracleCatalogueSchema.cpp
  COMMAND sed '/DROP_CTA_SQL_SCHEMA/r drop_oracle_catalogue_schema.cpp' ${CMAKE_CURRENT_SOURCE_DIR}/DropOracleCatalogueSchema.before_SQL.cpp > DropOracleCatalogueSchema.cpp
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/DropOracleCatalogueSchema.before_SQL.cpp drop_oracle_catalogue_schema.cpp)

add_custom_command(OUTPUT drop_sqlite_catalogue_schema.cpp
  COMMAND sed 's/^/\ \ \"/' ${CMAKE_CURRENT_SOURCE_DIR}/drop_sqlite_catalogue_schema.sql | sed 's/$$/\"/' > drop_sqlite_catalogue_schema.cpp
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/drop_sqlite_catalogue_schema.sql)

add_custom_command(OUTPUT DropSqliteCatalogueSchema.cpp
  COMMAND sed '/DROP_CTA_SQL_SCHEMA/r drop_sqlite_catalogue_schema.cpp' ${CMAKE_CURRENT_SOURCE_DIR}/DropSqliteCatalogueSchema.before_SQL.cpp > DropSqliteCatalogueSchema.cpp
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/DropSqliteCatalogueSchema.before_SQL.cpp drop_sqlite_catalogue_schema.cpp)

install (TARGETS cta-catalogue-schema-create DESTINATION /usr/bin)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-catalogue-schema-create.1cta DESTINATION /usr/share/man/man1)

add_executable(cta-catalogue-schema-drop
  DropOracleCatalogueSchema.cpp
  DropSchemaCmd.cpp
  DropSchemaCmdLineArgs.cpp
  DropSchemaCmdMain.cpp
  DropSqliteCatalogueSchema.cpp)

target_link_libraries (cta-catalogue-schema-drop
  ctacatalogue)

install (TARGETS cta-catalogue-schema-drop DESTINATION /usr/bin)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-catalogue-schema-drop.1cta DESTINATION /usr/share/man/man1)

add_executable(cta-database-poll
  PollDatabaseCmd.cpp
  PollDatabaseCmdLineArgs.cpp
  PollDatabaseCmdMain.cpp)

target_link_libraries (cta-database-poll
  ctacatalogue)

install (TARGETS cta-database-poll DESTINATION /usr/bin)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-database-poll.1cta DESTINATION /usr/share/man/man1)

add_executable(cta-catalogue-admin-user-create
  CreateAdminUserCmd.cpp
  CreateAdminUserCmdLineArgs.cpp
  CreateAdminUserCmdMain.cpp)

target_link_libraries (cta-catalogue-admin-user-create
  ctacatalogue)

install (TARGETS cta-catalogue-admin-user-create DESTINATION /usr/bin)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-catalogue-admin-user-create.1cta DESTINATION /usr/share/man/man1)

add_executable(cta-catalogue-admin-host-create
  CreateAdminHostCmd.cpp
  CreateAdminHostCmdLineArgs.cpp
  CreateAdminHostCmdMain.cpp)

target_link_libraries (cta-catalogue-admin-host-create
  ctacatalogue)

install (TARGETS cta-catalogue-admin-host-create DESTINATION /usr/bin)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-catalogue-admin-host-create.1cta DESTINATION /usr/share/man/man1)

set (CATALOGUE_CMD_LINE_UNIT_TESTS_LIB_SRC_FILES
  CreateAdminHostCmdLineArgs.cpp
  CreateAdminHostCmdLineArgsTest.cpp
  CreateAdminUserCmdLineArgs.cpp
  CreateAdminUserCmdLineArgsTest.cpp
  CreateSchemaCmdLineArgs.cpp
  CreateSchemaCmdLineArgsTest.cpp
  DeleteAllCatalogueDataCmdLineArgs.cpp
  DeleteAllCatalogueDataCmdLineArgsTest.cpp
  DropSchemaCmdLineArgs.cpp
  DropSchemaCmdLineArgsTest.cpp
  LockSchemaCmdLineArgs.cpp
  LockSchemaCmdLineArgsTest.cpp
  PollDatabaseCmdLineArgs.cpp
  PollDatabaseCmdLineArgsTest.cpp
  SchemaStatusCmdLineArgs.cpp
  SchemaStatusCmdLineArgsTest.cpp
  UnlockSchemaCmdLineArgs.cpp
  UnlockSchemaCmdLineArgsTest.cpp)

add_library (ctacataloguecmdlineunittests SHARED
  ${CATALOGUE_CMD_LINE_UNIT_TESTS_LIB_SRC_FILES})
install (TARGETS ctacataloguecmdlineunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
