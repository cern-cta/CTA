# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_manpage(cta-catalogue-schema-drop)

add_library(ctadropschemacmd SHARED
  DropSchemaCmd.cpp
  DropSchemaCmdLineArgs.cpp)
set_property(TARGET ctadropschemacmd PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctadropschemacmd PROPERTY VERSION "${CTA_LIBVERSION}")
target_link_libraries(ctadropschemacmd ctaschemachecker)

add_executable(cta-catalogue-schema-drop DropSchemaCmdMain.cpp)
target_link_libraries(cta-catalogue-schema-drop ctacatalogue ctadropschemacmd)

install(TARGETS ctadropschemacmd DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(TARGETS cta-catalogue-schema-drop DESTINATION ${CMAKE_INSTALL_BINDIR})
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-catalogue-schema-drop.1 DESTINATION ${CMAKE_INSTALL_MANDIR}/man1)
