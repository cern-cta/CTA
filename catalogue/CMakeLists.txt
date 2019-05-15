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

include_directories (${ORACLE-INSTANTCLIENT_INCLUDE_DIRS})

#
# Compiled protocol buffers (for ChecksumBlob)
#
include_directories(${CMAKE_BINARY_DIR}/eos_cta ${PROTOBUF3_INCLUDE_DIRS})

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wshadow")

set (CATALOGUE_LIB_SRC_FILES
  ArchiveFileBuilder.cpp
  ArchiveFileRow.cpp
  ArchiveFileItor.cpp
  ArchiveFileItorImpl.cpp
  Catalogue.cpp
  CatalogueFactory.cpp
  CatalogueFactoryFactory.cpp
  CatalogueSchema.cpp
  CmdLineTool.cpp
  FileSizeMismatch.cpp
  InMemoryCatalogue.cpp
  InMemoryCatalogueFactory.cpp
  MysqlCatalogue.cpp
  MysqlCatalogueFactory.cpp
  MysqlCatalogueSchema.cpp
  OracleCatalogue.cpp
  OracleCatalogueFactory.cpp
  PostgresCatalogue.cpp
  PostgresqlCatalogueFactory.cpp
  SqliteCatalogueSchema.cpp
  TapeFileWritten.cpp
  TapeItemImplementation.cpp
  TapePool.cpp
  RdbmsCatalogue.cpp
  RdbmsCatalogueGetArchiveFilesForRepackItor.cpp
  RdbmsCatalogueGetArchiveFilesItor.cpp
  SchemaCreatingSqliteCatalogue.cpp
  SqliteCatalogue.cpp
  SqliteCatalogueFactory.cpp
  TapeForWriting.cpp
  UserSpecifiedANonEmptyTape.cpp
  UserSpecifiedANonExistentTape.cpp
  UserSpecifiedAnEmptyStringComment.cpp
  UserSpecifiedAnEmptyStringDiskInstanceName.cpp
  UserSpecifiedAnEmptyStringLogicalLibraryName.cpp
  UserSpecifiedAnEmptyStringMediaType.cpp
  UserSpecifiedAnEmptyStringStorageClassName.cpp
  UserSpecifiedAnEmptyStringSupply.cpp
  UserSpecifiedAnEmptyStringTapePoolName.cpp
  UserSpecifiedAnEmptyStringUsername.cpp
  UserSpecifiedAnEmptyStringVendor.cpp
  UserSpecifiedAnEmptyStringVid.cpp
  UserSpecifiedAnEmptyStringVo.cpp
  UserSpecifiedAZeroCapacity.cpp
  UserSpecifiedAZeroCopyNb.cpp)

add_library (ctacatalogue SHARED
   ${CATALOGUE_LIB_SRC_FILES})

set_property(TARGET ctacatalogue PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctacatalogue PROPERTY   VERSION "${CTA_LIBVERSION}")

set_property(TARGET ctacatalogue APPEND PROPERTY INSTALL_RPATH ${ORACLE-INSTANTCLIENT_RPATH})
set_property(TARGET ctacatalogue APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})

install (TARGETS ctacatalogue DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

target_link_libraries (ctacatalogue
  ctacommon
  ctardbms)

add_custom_command (OUTPUT sqlite_catalogue_schema.sql mysql_catalogue_schema.sql oracle_catalogue_schema.sql postgres_catalogue_schema.sql
  COMMAND cat
    ${CMAKE_CURRENT_SOURCE_DIR}/sqlite_catalogue_schema_header.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/common_catalogue_schema.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/sqlite_catalogue_schema_trailer.sql
    | sed 's/NUMERIC\([^\)]*\)/INTEGER/g'
    | sed 's/CHECKSUM_BLOB_TYPE/BLOB\(200\)/g'
    > sqlite_catalogue_schema.sql
  COMMAND cat 
    ${CMAKE_CURRENT_SOURCE_DIR}/mysql_catalogue_schema_header.sql 
    ${CMAKE_CURRENT_SOURCE_DIR}/common_catalogue_schema.sql 
    ${CMAKE_CURRENT_SOURCE_DIR}/mysql_catalogue_schema_trailer.sql
    | sed 's/CHECKSUM_BLOB_TYPE/RAWBINARY\(200\)/g'
    > mysql_catalogue_schema.sql
  COMMAND cat
    ${CMAKE_CURRENT_SOURCE_DIR}/oracle_catalogue_schema_header.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/common_catalogue_schema.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/oracle_catalogue_schema_trailer.sql
    | sed 's/VARCHAR/VARCHAR2/g'
    | sed 's/CHECKSUM_BLOB_TYPE/RAW\(200\)/g'
    > oracle_catalogue_schema.sql
  COMMAND cat
    ${CMAKE_CURRENT_SOURCE_DIR}/postgres_catalogue_schema_header.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/common_catalogue_schema.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/postgres_catalogue_schema_trailer.sql
    | sed 's/CHECKSUM_BLOB_TYPE/BYTEA\(200\)/g'
    > postgres_catalogue_schema.sql
  DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/common_catalogue_schema.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/sqlite_catalogue_schema_header.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/sqlite_catalogue_schema_trailer.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/mysql_catalogue_schema_header.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/mysql_catalogue_schema_trailer.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/postgres_catalogue_schema_header.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/postgres_catalogue_schema_trailer.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/oracle_catalogue_schema_header.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/oracle_catalogue_schema_trailer.sql)

add_custom_command(OUTPUT sqlite_catalogue_schema.cpp
  COMMAND sed 's/^/\ \ \"/' sqlite_catalogue_schema.sql | sed 's/$$/\"/' > sqlite_catalogue_schema.cpp
  DEPENDS sqlite_catalogue_schema.sql)

add_custom_command(OUTPUT SqliteCatalogueSchema.cpp
  COMMAND sed '/CTA_SQL_SCHEMA/r sqlite_catalogue_schema.cpp' ${CMAKE_CURRENT_SOURCE_DIR}/SqliteCatalogueSchema.before_SQL.cpp > SqliteCatalogueSchema.cpp
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/SqliteCatalogueSchema.before_SQL.cpp sqlite_catalogue_schema.cpp)

# For Mysql
add_custom_command(OUTPUT mysql_catalogue_schema.cpp
  COMMAND sed 's/^/\ \ \"/' mysql_catalogue_schema.sql | sed 's/$$/\"/' > mysql_catalogue_schema.cpp
  DEPENDS mysql_catalogue_schema.sql)

add_custom_command(OUTPUT mysql_catalogue_schema_trigger.cpp
  COMMAND sed 's/^/\ \ \"/' ${CMAKE_CURRENT_SOURCE_DIR}/mysql_catalogue_schema_trigger.sql | sed 's/$$/\"/' > mysql_catalogue_schema_trigger.cpp
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/mysql_catalogue_schema_trigger.sql)


add_custom_command(OUTPUT MysqlCatalogueSchema.cpp
  COMMAND sed -e '/CTA_SQL_SCHEMA/r mysql_catalogue_schema.cpp' -e '/CTA_SQL_TRIGGER/r mysql_catalogue_schema_trigger.cpp' ${CMAKE_CURRENT_SOURCE_DIR}/MysqlCatalogueSchema.before_SQL.cpp > MysqlCatalogueSchema.cpp
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/MysqlCatalogueSchema.before_SQL.cpp mysql_catalogue_schema.cpp mysql_catalogue_schema_trigger.cpp)

# For Postgres
add_custom_command(OUTPUT postgres_catalogue_schema.cpp
  COMMAND sed 's/^/\ \ \"/' postgres_catalogue_schema.sql | sed 's/$$/\"/' > postgres_catalogue_schema.cpp
  DEPENDS postgres_catalogue_schema.sql)

add_custom_command(OUTPUT PostgresCatalogueSchema.cpp
  COMMAND sed -e '/CTA_SQL_SCHEMA/r postgres_catalogue_schema.cpp' ${CMAKE_CURRENT_SOURCE_DIR}/PostgresCatalogueSchema.before_SQL.cpp > PostgresCatalogueSchema.cpp
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/PostgresCatalogueSchema.before_SQL.cpp postgres_catalogue_schema.cpp)


set(IN_MEMORY_CATALOGUE_UNIT_TESTS_LIB_SRC_FILES
  CatalogueTest.cpp
  InMemoryCatalogueTest.cpp
  InMemoryVersionOfCatalogueTest.cpp)

add_library (ctainmemorycatalogueunittests SHARED
  ${IN_MEMORY_CATALOGUE_UNIT_TESTS_LIB_SRC_FILES})
set_property(TARGET ctainmemorycatalogueunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctainmemorycatalogueunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries (ctainmemorycatalogueunittests
  ctacatalogue)

install (TARGETS ctainmemorycatalogueunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

set (DBCONFIG_CATALOGUE_UNIT_TESTS_LIB_SRC_FILES
  CatalogueTest.cpp
  DbConfigVersionOfCatalogueTest.cpp)

add_library (ctadbconfigcatalogueunittests SHARED
  ${DBCONFIG_CATALOGUE_UNIT_TESTS_LIB_SRC_FILES})
set_property(TARGET ctadbconfigcatalogueunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctadbconfigcatalogueunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries (ctadbconfigcatalogueunittests
  ctacatalogue)

install (TARGETS ctadbconfigcatalogueunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

install (FILES cta-catalogue.conf.example
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

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
  SqliteCatalogueSchema.cpp
  PostgresCatalogueSchema.cpp
  MysqlCatalogueSchema.cpp)

target_link_libraries (cta-catalogue-schema-create
  ctacatalogue)

set_property (TARGET cta-catalogue-schema-create APPEND PROPERTY INSTALL_RPATH ${ORACLE-INSTANTCLIENT_RPATH})

install (TARGETS cta-catalogue-schema-create DESTINATION /usr/bin)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-catalogue-schema-create.1cta DESTINATION /usr/share/man/man1)

add_executable(cta-catalogue-schema-drop
  DropSchemaCmd.cpp
  DropSchemaCmdLineArgs.cpp
  DropSchemaCmdMain.cpp)

target_link_libraries (cta-catalogue-schema-drop
  ctacatalogue)

set_property (TARGET cta-catalogue-schema-drop APPEND PROPERTY INSTALL_RPATH ${ORACLE-INSTANTCLIENT_RPATH})

install (TARGETS cta-catalogue-schema-drop DESTINATION /usr/bin)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-catalogue-schema-drop.1cta DESTINATION /usr/share/man/man1)

add_executable(cta-database-poll
  PollDatabaseCmd.cpp
  PollDatabaseCmdLineArgs.cpp
  PollDatabaseCmdMain.cpp)

target_link_libraries (cta-database-poll
  ctacatalogue)

set_property (TARGET cta-database-poll APPEND PROPERTY INSTALL_RPATH ${ORACLE-INSTANTCLIENT_RPATH})

install (TARGETS cta-database-poll DESTINATION /usr/bin)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-database-poll.1cta DESTINATION /usr/share/man/man1)

add_executable(cta-catalogue-admin-user-create
  CreateAdminUserCmd.cpp
  CreateAdminUserCmdLineArgs.cpp
  CreateAdminUserCmdMain.cpp)

target_link_libraries (cta-catalogue-admin-user-create
  ctacatalogue)

set_property (TARGET cta-catalogue-admin-user-create APPEND PROPERTY INSTALL_RPATH ${ORACLE-INSTANTCLIENT_RPATH})

install (TARGETS cta-catalogue-admin-user-create DESTINATION /usr/bin)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-catalogue-admin-user-create.1cta DESTINATION /usr/share/man/man1)

add_executable(cta-catalogue-schema-verify
  VerifySchemaCmd.cpp
  VerifySchemaCmdLineArgs.cpp
  VerifySchemaCmdMain.cpp
  CatalogueSchema.cpp
  OracleCatalogueSchema.cpp
  SqliteCatalogueSchema.cpp
  PostgresCatalogueSchema.cpp
  MysqlCatalogueSchema.cpp)

target_link_libraries (cta-catalogue-schema-verify
  ctacatalogue)

set_property (TARGET cta-catalogue-schema-verify APPEND PROPERTY INSTALL_RPATH ${ORACLE-INSTANTCLIENT_RPATH})

install (TARGETS cta-catalogue-schema-verify DESTINATION /usr/bin)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-catalogue-schema-verify.1cta DESTINATION /usr/share/man/man1)


set (CATALOGUE_CMD_LINE_UNIT_TESTS_LIB_SRC_FILES
  CreateAdminUserCmdLineArgs.cpp
  CreateAdminUserCmdLineArgsTest.cpp
  CreateSchemaCmdLineArgs.cpp
  CreateSchemaCmdLineArgsTest.cpp
  VerifySchemaCmdLineArgs.cpp
  VerifySchemaCmdLineArgsTest.cpp
  DropSchemaCmdLineArgs.cpp
  DropSchemaCmdLineArgsTest.cpp
  PollDatabaseCmdLineArgs.cpp
  PollDatabaseCmdLineArgsTest.cpp)

add_library (ctacataloguecmdlineunittests SHARED
  ${CATALOGUE_CMD_LINE_UNIT_TESTS_LIB_SRC_FILES})
set_property(TARGET ctacataloguecmdlineunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctacataloguecmdlineunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

install (TARGETS ctacataloguecmdlineunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
