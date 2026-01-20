# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

cmake_minimum_required (VERSION 3.17)

find_package(postgres REQUIRED)

include_directories (${POSTGRES_INCLUDE_DIRS})

#
# Compiled protocol buffers (for ChecksumBlob)
#

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/insert_cta_scheduler_version.sql.in insert_cta_scheduler_version.sql.filled.in)

add_custom_command(OUTPUT postgres_scheduler_schema.cpp.in
  COMMAND sed 's/^/\ \ \"/' insert_cta_scheduler_version.sql.filled.in ${CMAKE_CURRENT_SOURCE_DIR}/postgres_scheduler_schema.sql | sed 's/$$/\"/' > postgres_scheduler_schema.cpp
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/postgres_scheduler_schema.sql insert_cta_scheduler_version.sql.filled.in)

add_custom_command(OUTPUT PostgresSchedulerSchema.cpp
  COMMAND sed -e '/CTA_SQL_SCHEMA/r postgres_scheduler_schema.cpp.in' ${CMAKE_CURRENT_SOURCE_DIR}/PostgresSchedulerSchema.before_SQL.cpp > PostgresSchedulerSchema.cpp
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/PostgresSchedulerSchema.before_SQL.cpp postgres_scheduler_schema.cpp.in)

# Create a library for the schema sources so they can be reused
add_library(ctaschedulerschema
  PostgresSchedulerSchema.cpp
  SchedulerSchema.cpp
)
target_link_libraries(ctaschedulerschema ctacatalogue)

set (SCHEMA_CREATE_SRC_FILES
  CmdLineTool.cpp
  CreateSchemaCmd.cpp
  CreateSchemaCmdLineArgs.cpp
  CreateSchemaCmdMain.cpp
)

add_executable(cta-scheduler-schema-create ${SCHEMA_CREATE_SRC_FILES})
target_link_libraries (cta-scheduler-schema-create ctaschedulerschema ctacatalogue)

if(CTA_USE_PGSCHED)
  install (TARGETS cta-scheduler-schema-create DESTINATION /usr/bin)
endif()

set (DROP_SCHEMA_CMD
  CmdLineTool.cpp
  DropSchemaCmd.cpp
  DropSchemaCmdLineArgs.cpp
  DropSchemaCmdMain.cpp
)

add_executable(cta-scheduler-schema-drop ${DROP_SCHEMA_CMD})
target_link_libraries (cta-scheduler-schema-drop ctaschedulerschema ctacatalogue)

if(CTA_USE_PGSCHED)
  install (TARGETS cta-scheduler-schema-drop DESTINATION /usr/bin)
endif()
