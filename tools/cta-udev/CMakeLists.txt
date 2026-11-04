# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Udev helpers live under lib/udev, including on platforms using lib64 for libraries.
install(FILES src/62-cta.rules DESTINATION lib/udev/rules.d)
install(FILES cta-udev.example.ini DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/doc/cta-udev/examples)
install(PROGRAMS src/cta_udev.py DESTINATION lib/udev RENAME cta-udev)

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
