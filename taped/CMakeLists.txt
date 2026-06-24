# SPDX-FileCopyrightText: 2021 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

add_subdirectory (scsi)
add_subdirectory (drive)
add_subdirectory (system)
add_subdirectory (file)
add_subdirectory (session)
add_subdirectory (rao)
add_subdirectory (daemon)

add_executable (cta-taped cta-taped.cpp)
target_link_libraries(cta-taped ctatapedaemon ctacommon)

add_manpage(cta-taped)

install (TARGETS cta-taped DESTINATION usr/bin)
install (FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-taped.1cta DESTINATION /usr/share/man/man1)
install (FILES cta-taped.logrotate DESTINATION /etc/logrotate.d RENAME cta-taped)
install (FILES cta-taped.sysconfig DESTINATION /etc/sysconfig RENAME cta-taped)
install (FILES cta-taped.service DESTINATION /etc/systemd/system)
