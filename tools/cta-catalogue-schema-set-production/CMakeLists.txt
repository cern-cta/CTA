# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_manpage(cta-catalogue-schema-set-production)

add_executable(cta-catalogue-schema-set-production
  SetProductionCmd.cpp
  SetProductionCmdLineArgs.cpp
  SetProductionCmdMain.cpp)

target_link_libraries(cta-catalogue-schema-set-production ctacatalogue ctaschemachecker)

install(TARGETS cta-catalogue-schema-set-production DESTINATION /usr/bin)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-catalogue-schema-set-production.1cta DESTINATION /usr/share/man/man1)
