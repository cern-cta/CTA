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

add_subdirectory(cta-catalogue-schema)

find_package(postgres REQUIRED)

include_directories (${POSTGRES_INCLUDE_DIRS})

if (OCCI_SUPPORT)
  find_package (oracle-instantclient REQUIRED)
  include_directories (${ORACLE-INSTANTCLIENT_INCLUDE_DIRS})
  add_definitions( -DSUPPORT_OCCI )
endif ()

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wshadow")

file (GLOB CATALOGUE_LIB_SRC_FILES
  ArchiveFileRow.cpp
  ArchiveFileRowWithoutTimestamps.cpp
  CatalogueFactoryFactory.cpp
  CatalogueSchema.cpp
  CmdLineTool.cpp
  DriveConfig.cpp
  dummy/*.cpp
  rdbms/*.cpp
  retrywrappers/*.cpp
  TapeDrivesCatalogueState.cpp
  TapeFileWritten.cpp
  TapeForWriting.cpp
  TapeItemImplementation.cpp
  TapePool.cpp
)

# It use set because it was giving linking error
set (CATALOGUE_LIB_SRC_FILES
  ${CATALOGUE_LIB_SRC_FILES}
  SchemaCreatingSqliteCatalogue.cpp
  SchemaVersion.cpp
  SqliteCatalogueSchema.cpp
)

# OCCI
if (OCCI_SUPPORT)
  file (GLOB CATALOGUE_OCCI_LIB_SRC_FILES
    OracleCatalogueFactory.cpp
    rdbms/oracle/*.cpp
  )

  add_library (ctacatalogueocci SHARED
    ${CATALOGUE_OCCI_LIB_SRC_FILES})
  set_property(TARGET ctacatalogueocci PROPERTY SOVERSION "${CTA_SOVERSION}")
  set_property(TARGET ctacatalogueocci PROPERTY   VERSION "${CTA_LIBVERSION}")

  target_compile_definitions(ctacatalogueocci PRIVATE VERSION_API="${CTA_VERSION}-${CTA_RELEASE}")

  target_link_libraries (ctacatalogueocci
    ctardbmsocci
  )

  install(TARGETS ctacatalogueocci DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
endif()

# IN_MEMORY
file (GLOB CATALOGUE_IN_MEMORY_LIB_SRC_FILES
  InMemoryCatalogue.cpp
  InMemoryCatalogueFactory.cpp
)

add_library (ctacatalogueinmemory SHARED
  ${CATALOGUE_IN_MEMORY_LIB_SRC_FILES})
set_property(TARGET ctacatalogueinmemory PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctacatalogueinmemory PROPERTY   VERSION "${CTA_LIBVERSION}")

target_compile_definitions(ctacatalogueinmemory PRIVATE VERSION_API="${CTA_VERSION}-${CTA_RELEASE}")

install(TARGETS ctacatalogueinmemory DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

# SQLITE - Sqlite file based databases are not supported 
file (GLOB CATALOGUE_SQLITE_LIB_SRC_FILES
  rdbms/sqlite/*.cpp
)

add_library (ctacataloguesqlite SHARED
  ${CATALOGUE_SQLITE_LIB_SRC_FILES})
set_property(TARGET ctacataloguesqlite PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctacataloguesqlite PROPERTY   VERSION "${CTA_LIBVERSION}")

target_compile_definitions(ctacataloguesqlite PRIVATE VERSION_API="${CTA_VERSION}-${CTA_RELEASE}")

target_link_libraries (ctacataloguesqlite
  ctardbmssqlite
)

install(TARGETS ctacataloguesqlite DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

# POSTGRES
file (GLOB CATALOGUE_POSTGRES_LIB_SRC_FILES
  PostgresqlCatalogueFactory.cpp
  rdbms/postgres/*.cpp
)

add_library (ctacataloguepostrgres SHARED
  ${CATALOGUE_POSTGRES_LIB_SRC_FILES})
set_property(TARGET ctacataloguepostrgres PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctacataloguepostrgres PROPERTY   VERSION "${CTA_LIBVERSION}")

target_compile_definitions(ctacataloguepostrgres PRIVATE VERSION_API="${CTA_VERSION}-${CTA_RELEASE}")

target_link_libraries (ctacataloguepostrgres
  ctardbmspostgres
)

install(TARGETS ctacataloguepostrgres DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

# COMMON
add_library (ctacatalogue SHARED
  ${CATALOGUE_LIB_SRC_FILES}
)

set_property(TARGET ctacatalogue PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctacatalogue PROPERTY   VERSION "${CTA_LIBVERSION}")

target_compile_definitions(ctacatalogue PRIVATE VERSION_API="${CTA_VERSION}-${CTA_RELEASE}")

install (TARGETS ctacatalogue DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

target_link_libraries (ctacatalogue
  ctacommon
  ctardbms
  ctacataloguepostrgres
  ctacatalogueinmemory
  ctacataloguesqlite
)

if (OCCI_SUPPORT)
  target_link_libraries (ctacatalogue ctacatalogueocci
)
endif()

add_custom_command(
  OUTPUT AllCatalogueSchema.hpp
  COMMAND ${CMAKE_COMMAND} -E env "LC_ALL=C" sh ${CMAKE_CURRENT_SOURCE_DIR}/CreateAllSchemasCppFile.sh ${CMAKE_CURRENT_SOURCE_DIR}
  DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/CreateAllSchemasCppFile.sh
    ${CMAKE_CURRENT_SOURCE_DIR}/AllCatalogueSchema.hpp.in
)

add_custom_command(
  OUTPUT sqlite_catalogue_schema.cpp postgres_catalogue_schema.cpp
  COMMAND ${CMAKE_COMMAND} -E env "LC_ALL=C" sed 's/^/\ \ \"/' cta-catalogue-schema/sqlite_catalogue_schema.sql | sed 's/$$/\"/' > sqlite_catalogue_schema.cpp
  COMMAND ${CMAKE_COMMAND} -E env "LC_ALL=C" sed 's/^/\ \ \"/' cta-catalogue-schema/postgres_catalogue_schema.sql | sed 's/$$/\"/' > postgres_catalogue_schema.cpp
  DEPENDS
    create_all_schemas_SQL
    ${CMAKE_CURRENT_BINARY_DIR}/cta-catalogue-schema/sqlite_catalogue_schema.sql
    ${CMAKE_CURRENT_BINARY_DIR}/cta-catalogue-schema/postgres_catalogue_schema.sql
)

add_custom_command(
  OUTPUT SqliteCatalogueSchema.cpp PostgresCatalogueSchema.cpp
  COMMAND ${CMAKE_COMMAND} -E env "LC_ALL=C" sed '/CTA_SQL_SCHEMA/r sqlite_catalogue_schema.cpp' ${CMAKE_CURRENT_SOURCE_DIR}/SqliteCatalogueSchema.before_SQL.cpp > SqliteCatalogueSchema.cpp
  COMMAND ${CMAKE_COMMAND} -E env "LC_ALL=C" sed -e '/CTA_SQL_SCHEMA/r postgres_catalogue_schema.cpp' ${CMAKE_CURRENT_SOURCE_DIR}/PostgresCatalogueSchema.before_SQL.cpp > PostgresCatalogueSchema.cpp
  DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/SqliteCatalogueSchema.before_SQL.cpp sqlite_catalogue_schema.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/PostgresCatalogueSchema.before_SQL.cpp postgres_catalogue_schema.cpp
)

add_custom_target(create_pre_sqlit_and_postgres_catalogue_schema_cpp ALL
  DEPENDS
    ${CMAKE_CURRENT_BINARY_DIR}/PostgresCatalogueSchema.cpp
    ${CMAKE_CURRENT_BINARY_DIR}/SqliteCatalogueSchema.cpp
)

if (OCCI_SUPPORT)
  set(ORACLE_SCHEMA_CPP ${CMAKE_CURRENT_BINARY_DIR}/oracle_catalogue_schema.cpp)
  set(ORACLE_CATALOGUE_SCHEMA_CPP ${CMAKE_CURRENT_BINARY_DIR}/OracleCatalogueSchema.cpp)

  add_custom_command(
    OUTPUT ${ORACLE_SCHEMA_CPP}
    COMMAND ${CMAKE_COMMAND} -E env "LC_ALL=C" sed 's/^/\ \ \"/' ${CMAKE_CURRENT_BINARY_DIR}/cta-catalogue-schema/oracle_catalogue_schema.sql | sed 's/$$/\"/' > ${ORACLE_SCHEMA_CPP}
    DEPENDS
      create_all_schemas_SQL
      ${CMAKE_CURRENT_BINARY_DIR}/cta-catalogue-schema/oracle_catalogue_schema.sql
    COMMENT "Creating oracle_catalogue_schema.cpp"
  )

  add_custom_command(
    OUTPUT ${ORACLE_CATALOGUE_SCHEMA_CPP}
    COMMAND ${CMAKE_COMMAND} -E env "LC_ALL=C" sed '/CTA_SQL_SCHEMA/r ${ORACLE_SCHEMA_CPP}' ${CMAKE_CURRENT_SOURCE_DIR}/OracleCatalogueSchema.before_SQL.cpp > ${ORACLE_CATALOGUE_SCHEMA_CPP}
    DEPENDS
      ${ORACLE_SCHEMA_CPP}
      ${CMAKE_CURRENT_SOURCE_DIR}/OracleCatalogueSchema.before_SQL.cpp
    COMMENT "Creating OracleCatalogueSchema.cpp"
  )

  add_custom_target(create_pre_oracle_catalogue_schema_cpp ALL
    DEPENDS
      ${ORACLE_CATALOGUE_SCHEMA_CPP}
  )
endif ()

file (GLOB CATALOGUE_MODULES_TESTS_SRC_FILES
  tests/modules/*.cpp
)

set(IN_MEMORY_CATALOGUE_UNIT_TESTS_LIB_SRC_FILES
  ${CATALOGUE_MODULES_TESTS_SRC_FILES}
  TapeItemWrittenPointerTest.cpp
  tests/CatalogueTestUtils.cpp
  tests/InMemoryCatalogueTest.cpp
  tests/InMemoryVersionOfCatalogueTest.cpp
)

add_library (ctainmemorycatalogueunittests SHARED
  ${IN_MEMORY_CATALOGUE_UNIT_TESTS_LIB_SRC_FILES})
set_property(TARGET ctainmemorycatalogueunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctainmemorycatalogueunittests PROPERTY   VERSION "${CTA_LIBVERSION}")
# The test "filesWrittenToTape_many_archive_files" is over 1000 lines long and exceeds the variable tracking size limit.
# We switch off this compiler option for this file to avoid compiling it twice.
set_property(TARGET ctainmemorycatalogueunittests PROPERTY COMPILE_FLAGS "-fno-var-tracking-assignments")

target_link_libraries (ctainmemorycatalogueunittests
  ctacatalogue ctadisk)

install (TARGETS ctainmemorycatalogueunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

set (DBCONFIG_CATALOGUE_UNIT_TESTS_LIB_SRC_FILES
  ${CATALOGUE_MODULES_TESTS_SRC_FILES}
  tests/CatalogueTestUtils.cpp
  tests/DbConfigVersionOfCatalogueTest.cpp
)

add_library (ctadbconfigcatalogueunittests SHARED
  ${DBCONFIG_CATALOGUE_UNIT_TESTS_LIB_SRC_FILES})
set_property(TARGET ctadbconfigcatalogueunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctadbconfigcatalogueunittests PROPERTY   VERSION "${CTA_LIBVERSION}")
# The test "filesWrittenToTape_many_archive_files" is over 1000 lines long and exceeds the variable tracking size limit.
# We switch off this compiler option for this file to avoid compiling it twice.
set_property(TARGET ctadbconfigcatalogueunittests PROPERTY COMPILE_FLAGS "-fno-var-tracking-assignments")

target_link_libraries (ctadbconfigcatalogueunittests
  ctacatalogue)

install (TARGETS ctadbconfigcatalogueunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

install (FILES cta-catalogue.conf.example
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

set (SCHEMA_CREATE_SRC_FILES
  AllCatalogueSchema.hpp
  CreateSchemaCmd.cpp
  CreateSchemaCmdLineArgs.cpp
  CreateSchemaCmdMain.cpp
  VersionedCatalogueSchemas.cpp
  SqliteCatalogueSchema.cpp
  PostgresCatalogueSchema.cpp
)

if (OCCI_SUPPORT)
  set (SCHEMA_CREATE_SRC_FILES
    ${SCHEMA_CREATE_SRC_FILES}
    OracleCatalogueSchema.cpp)
endif()

add_executable(cta-catalogue-schema-create ${SCHEMA_CREATE_SRC_FILES})

set (SCHEMA_CHECKER_LIB_SRC_FILES
  AllCatalogueSchema.hpp
  SQLiteSchemaInserter.cpp
  SchemaSqlStatementsReader.cpp
  SchemaComparer.cpp
  SQLiteSchemaComparer.cpp
  DbToSQLiteStatementTransformer.cpp
  SchemaCheckerResult.cpp
  SchemaChecker.cpp
  DatabaseMetadataGetter.cpp
  CatalogueSchema.cpp
  SqliteCatalogueSchema.cpp
  PostgresCatalogueSchema.cpp
)

if (OCCI_SUPPORT)
  set (SCHEMA_CHECKER_LIB_SRC_FILES
    ${SCHEMA_CHECKER_LIB_SRC_FILES}
    OracleCatalogueSchema.cpp)
endif()

add_library (ctaschemachecker SHARED
   ${SCHEMA_CHECKER_LIB_SRC_FILES})

install (TARGETS ctaschemachecker DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

target_link_libraries (cta-catalogue-schema-create ctacatalogue)

install (TARGETS cta-catalogue-schema-create DESTINATION /usr/bin)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-catalogue-schema-create.1cta DESTINATION /usr/share/man/man1)

set (DROP_SCHEMA_CMD
  DropSchemaCmd.cpp
  DropSchemaCmdLineArgs.cpp
)
add_library (ctadropschemacmd SHARED ${DROP_SCHEMA_CMD})
set_property(TARGET ctadropschemacmd PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctadropschemacmd PROPERTY   VERSION "${CTA_LIBVERSION}")
install (TARGETS ctadropschemacmd DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
target_link_libraries (ctadropschemacmd ctaschemachecker)

add_executable(cta-catalogue-schema-drop DropSchemaCmdMain.cpp)

target_link_libraries (cta-catalogue-schema-drop ctacatalogue ctadropschemacmd)

install (TARGETS cta-catalogue-schema-drop DESTINATION /usr/bin)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-catalogue-schema-drop.1cta DESTINATION /usr/share/man/man1)

add_executable(cta-catalogue-schema-set-production
  SetProductionCmd.cpp
  SetProductionCmdMain.cpp
  SetProductionCmdLineArgs.cpp
)

target_link_libraries (cta-catalogue-schema-set-production ctacatalogue ctaschemachecker)

install (TARGETS cta-catalogue-schema-set-production DESTINATION /usr/bin)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-catalogue-schema-set-production.1cta DESTINATION /usr/share/man/man1)

add_executable(cta-database-poll
  PollDatabaseCmd.cpp
  PollDatabaseCmdLineArgs.cpp
  PollDatabaseCmdMain.cpp)

target_link_libraries (cta-database-poll ctacatalogue)

install (TARGETS cta-database-poll DESTINATION /usr/bin)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-database-poll.1cta DESTINATION /usr/share/man/man1)

add_executable(cta-catalogue-admin-user-create
  CreateAdminUserCmd.cpp
  CreateAdminUserCmdLineArgs.cpp
  CreateAdminUserCmdMain.cpp)

target_link_libraries(cta-catalogue-admin-user-create ctacatalogue)

install(TARGETS cta-catalogue-admin-user-create DESTINATION /usr/bin)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-catalogue-admin-user-create.1cta DESTINATION /usr/share/man/man1)

add_executable(cta-catalogue-schema-verify
  VerifySchemaCmd.cpp
  VerifySchemaCmdLineArgs.cpp
  VerifySchemaCmdMain.cpp)

target_link_libraries(cta-catalogue-schema-verify ctacatalogue ctaschemachecker)

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
