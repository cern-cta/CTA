# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_executable(cta-scheduler-schema-drop
  DropSchemaCmd.cpp
  DropSchemaCmdLineArgs.cpp
  DropSchemaCmdMain.cpp)

target_link_libraries(cta-scheduler-schema-drop ctaschedulerschema ctacatalogue)

install(TARGETS cta-scheduler-schema-drop DESTINATION ${CMAKE_INSTALL_BINDIR})

add_manpage(cta-scheduler-schema-drop)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-scheduler-schema-drop.1 DESTINATION ${CMAKE_INSTALL_MANDIR}/man1)
