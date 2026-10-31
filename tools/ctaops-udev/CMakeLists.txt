# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Udev helpers live under lib/udev, including on platforms using lib64 for libraries.
install(FILES src/62-ctaops.rules DESTINATION lib/udev/rules.d)
install(FILES src/ctaops-udev.ini DESTINATION lib/udev)
install(PROGRAMS src/ctaops_udev.py DESTINATION lib/udev RENAME ctaops-udev)

set(manpage "${CMAKE_CURRENT_BINARY_DIR}/ctaops-udev.8ctaops")
add_custom_command(
  OUTPUT "${manpage}"
  COMMAND "${PANDOC_EXECUTABLE}" --standalone --to man
    --output "${manpage}" "${CMAKE_CURRENT_SOURCE_DIR}/ctaops-udev.8ctaops.md"
  DEPENDS ctaops-udev.8ctaops.md
  COMMENT "Building ctaops-udev(8ctaops) man page"
  VERBATIM
)
add_custom_target(ctaops-udev_man_page ALL DEPENDS "${manpage}")
install(FILES "${manpage}" DESTINATION ${CMAKE_INSTALL_MANDIR}/man8)
