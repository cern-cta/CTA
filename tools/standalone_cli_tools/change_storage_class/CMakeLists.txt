# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

cmake_minimum_required (VERSION 3.17)

add_executable(cta-change-storage-class ChangeStorageClass.cpp ChangeStorageClassMain.cpp ../../CtaAdminCmdParser.cpp JsonFileData.cpp)
target_link_libraries(cta-change-storage-class ctacommon stdc++fs ctaCmdlineToolsCommon)

add_manpage(cta-change-storage-class)

install(TARGETS cta-change-storage-class DESTINATION usr/bin)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-change-storage-class.1cta DESTINATION /usr/share/man/man1)
