# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_manpage(cta-catalogue-schema-verify)

add_executable(cta-catalogue-schema-verify
  VerifySchemaCmd.cpp
  VerifySchemaCmdLineArgs.cpp
  VerifySchemaCmdMain.cpp)

target_link_libraries(cta-catalogue-schema-verify ctacatalogue ctaschemachecker)

install(TARGETS cta-catalogue-schema-verify DESTINATION ${CMAKE_INSTALL_BINDIR})
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-catalogue-schema-verify.1 DESTINATION ${CMAKE_INSTALL_MANDIR}/man1)
