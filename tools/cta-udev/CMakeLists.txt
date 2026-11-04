# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

install(FILES src/62-cta.rules DESTINATION ${CTA_UDEV_DIR}/rules.d)
install(DIRECTORY DESTINATION ${CMAKE_INSTALL_FULL_SYSCONFDIR}/cta/library-inventory)
install(FILES cta-udev.example.ini DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/doc/cta-udev/examples)
install(PROGRAMS src/cta_udev.py DESTINATION ${CTA_UDEV_DIR} RENAME cta-udev)

set(manpage "${CMAKE_CURRENT_BINARY_DIR}/cta-udev.8cta")
add_custom_command(
  OUTPUT "${manpage}"
  COMMAND "${PANDOC_EXECUTABLE}" --standalone --to man
    --output "${manpage}" "${CMAKE_CURRENT_SOURCE_DIR}/cta-udev.8cta.md"
  DEPENDS cta-udev.8cta.md
  COMMENT "Building cta-udev(8cta) man page"
  VERBATIM
)
add_custom_target(cta-udev_man_page ALL DEPENDS "${manpage}")
install(FILES "${manpage}" DESTINATION ${CMAKE_INSTALL_MANDIR}/man8)
