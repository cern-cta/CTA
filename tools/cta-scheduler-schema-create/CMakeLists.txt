# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_executable(cta-scheduler-schema-create
  CreateSchemaCmd.cpp
  CreateSchemaCmdLineArgs.cpp
  CreateSchemaCmdMain.cpp)

target_link_libraries(cta-scheduler-schema-create ctaschedulerschema ctacatalogue)

install(TARGETS cta-scheduler-schema-create DESTINATION ${CMAKE_INSTALL_BINDIR})

add_manpage(cta-scheduler-schema-create)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-scheduler-schema-create.1 DESTINATION ${CMAKE_INSTALL_MANDIR}/man1)
