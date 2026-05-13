# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

cmake_minimum_required (VERSION 3.17)

add_executable(cta-restore-deleted-files RestoreFilesCmdMain.cpp RestoreFilesCmd.cpp ../../CtaAdminCmdParser.cpp)

# To prevent failing due to warning
# #warning "<ciso646> is not a standard header since C++20, use <version> to detect implementation-specific macros"
target_compile_options(cta-restore-deleted-files PRIVATE $<$<CXX_COMPILER_ID:GNU>:-Wno-error=cpp>)
target_link_libraries(cta-restore-deleted-files
  ctaCmdlineToolsCommon)

add_manpage(cta-restore-deleted-files)

install(TARGETS cta-restore-deleted-files DESTINATION usr/bin)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-restore-deleted-files.1cta DESTINATION /usr/share/man/man1)
