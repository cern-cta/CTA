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

# Commenting out Protobuf - to be removed if possible
# find_package(Protobuf3 REQUIRED)
find_package(postgres REQUIRED)

include_directories (${POSTGRES_INCLUDE_DIRS})

#
# Compiled protocol buffers (for ChecksumBlob)
# in 2023 seems unnecessary we can insert checksumblob without protobuf as bytearray
# commenting out for the moment to remove protobuf if possible from PGSCHED DB implementation
#
#include_directories(${CMAKE_BINARY_DIR}/eos_cta ${PROTOBUF3_INCLUDE_DIRS})

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/insert_cta_scheduler_version.sql.in insert_cta_scheduler_version.sql)

add_custom_command(OUTPUT postgres_scheduler_schema.cpp
  COMMAND sed 's/^/\ \ \"/' ${CMAKE_CURRENT_SOURCE_DIR}/postgres_scheduler_schema.sql insert_cta_scheduler_version.sql | sed 's/$$/\"/' > postgres_scheduler_schema.cpp
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/postgres_scheduler_schema.sql insert_cta_scheduler_version.sql)

add_custom_command(OUTPUT PostgresSchedulerSchema.cpp
  COMMAND sed -e '/CTA_SQL_SCHEMA/r postgres_scheduler_schema.cpp' ${CMAKE_CURRENT_SOURCE_DIR}/PostgresSchedulerSchema.before_SQL.cpp > PostgresSchedulerSchema.cpp
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/PostgresSchedulerSchema.before_SQL.cpp postgres_scheduler_schema.cpp)

set (SCHEMA_CREATE_SRC_FILES
  CmdLineTool.cpp
  CreateSchemaCmd.cpp
  CreateSchemaCmdLineArgs.cpp
  CreateSchemaCmdMain.cpp
  PostgresSchedulerSchema.cpp
  SchedulerSchema.cpp
)

add_executable(cta-scheduler-schema-create ${SCHEMA_CREATE_SRC_FILES})
#target_link_libraries (cta-scheduler-schema-create PRIVATE ctacatalogue)

# Protobuf shall not bee needed for Postgres Scheduler
# set_property(TARGET cta-scheduler-schema-create APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})

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
#target_link_libraries (cta-scheduler-schema-drop PRIVATE ctacatalogue)

if(CTA_USE_PGSCHED)
  install (TARGETS cta-scheduler-schema-drop DESTINATION /usr/bin)
endif()