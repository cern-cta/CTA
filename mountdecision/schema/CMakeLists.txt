# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

cmake_minimum_required (VERSION 3.17)

add_library(ctamountdecisionschema
  MountDecisionSchema.cpp
  SchemaUtils.cpp)

target_link_libraries(ctamountdecisionschema
  ctacommon
  ctardbms)

set (SCHEMA_CREATE_SRC_FILES
  CreateSchemaCmd.cpp)

add_executable(cta-mount-decision-schema-create ${SCHEMA_CREATE_SRC_FILES})
target_link_libraries (cta-mount-decision-schema-create ctamountdecisionschema)

install (TARGETS cta-mount-decision-schema-create DESTINATION /usr/bin)

set (DROP_SCHEMA_CMD
  DropSchemaCmd.cpp)

add_executable(cta-mount-decision-schema-drop ${DROP_SCHEMA_CMD})
target_link_libraries (cta-mount-decision-schema-drop ctamountdecisionschema)

install (TARGETS cta-mount-decision-schema-drop DESTINATION /usr/bin)
