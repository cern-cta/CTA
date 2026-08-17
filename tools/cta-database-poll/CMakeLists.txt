# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_manpage(cta-database-poll)

add_executable(cta-database-poll
  PollDatabaseCmd.cpp
  PollDatabaseCmdLineArgs.cpp
  PollDatabaseCmdMain.cpp)

target_link_libraries(cta-database-poll ctacatalogue)

install(TARGETS cta-database-poll DESTINATION /usr/bin)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-database-poll.1cta DESTINATION /usr/share/man/man1)
