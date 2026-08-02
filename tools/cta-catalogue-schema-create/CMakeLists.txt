# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_manpage(cta-catalogue-schema-create)

set (SCHEMA_CREATE_SRC_FILES
  CreateSchemaCmd.cpp
  CreateSchemaCmdLineArgs.cpp
  CreateSchemaCmdMain.cpp
  ${PROJECT_SOURCE_DIR}/catalogue/VersionedCatalogueSchemas.cpp
  ${CMAKE_BINARY_DIR}/catalogue/SqliteCatalogueSchema.cpp
  ${CMAKE_BINARY_DIR}/catalogue/PostgresCatalogueSchema.cpp)

set_source_files_properties(
  ${CMAKE_BINARY_DIR}/catalogue/SqliteCatalogueSchema.cpp
  ${CMAKE_BINARY_DIR}/catalogue/PostgresCatalogueSchema.cpp
  PROPERTIES GENERATED TRUE)

if (OCCI_SUPPORT)
  list(APPEND SCHEMA_CREATE_SRC_FILES
    ${CMAKE_BINARY_DIR}/catalogue/OracleCatalogueSchema.cpp)
  set_source_files_properties(
    ${CMAKE_BINARY_DIR}/catalogue/OracleCatalogueSchema.cpp
    PROPERTIES GENERATED TRUE)
endif()

add_executable(cta-catalogue-schema-create ${SCHEMA_CREATE_SRC_FILES})
add_dependencies(cta-catalogue-schema-create
  create_all_catalogue_schema_header
  create_pre_sqlit_and_postgres_catalogue_schema_cpp)

if (OCCI_SUPPORT)
  target_compile_definitions(cta-catalogue-schema-create PRIVATE SUPPORT_OCCI)
  add_dependencies(cta-catalogue-schema-create create_pre_oracle_catalogue_schema_cpp)
endif()

target_link_libraries(cta-catalogue-schema-create ctacatalogue)

install(TARGETS cta-catalogue-schema-create DESTINATION /usr/bin)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-catalogue-schema-create.1cta DESTINATION /usr/share/man/man1)
