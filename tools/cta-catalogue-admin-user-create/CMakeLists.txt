# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_manpage(cta-catalogue-admin-user-create)

add_executable(cta-catalogue-admin-user-create
  CreateAdminUserCmd.cpp
  CreateAdminUserCmdLineArgs.cpp
  CreateAdminUserCmdMain.cpp)

target_link_libraries(cta-catalogue-admin-user-create ctacatalogue)

install(TARGETS cta-catalogue-admin-user-create DESTINATION ${CMAKE_INSTALL_BINDIR})
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-catalogue-admin-user-create.1 DESTINATION ${CMAKE_INSTALL_MANDIR}/man1)
