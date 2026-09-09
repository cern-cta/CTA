# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_executable(cta-scheduler-schema-drop
  DropSchemaCmd.cpp
  DropSchemaCmdLineArgs.cpp
  DropSchemaCmdMain.cpp)

target_link_libraries(cta-scheduler-schema-drop ctaschedulerschema ctacatalogue)

install(TARGETS cta-scheduler-schema-drop DESTINATION /usr/bin)
