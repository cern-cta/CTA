# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set(restore_tool cta-eos-restore-files)
set(restore_artifact_dir "${CMAKE_CURRENT_BINARY_DIR}/restore-tool-artifacts")
file(MAKE_DIRECTORY "${restore_artifact_dir}")

# The relative symlink also serves as the installed multicall entry point.
file(CREATE_LINK "${restore_tool}" "${CMAKE_CURRENT_BINARY_DIR}/${restore_tool}-env" SYMBOLIC)
add_custom_target(cta-restore-tool-artifacts ALL
  COMMAND "${CMAKE_COMMAND}" -E rm -rf "${restore_artifact_dir}/man"
  COMMAND "${CMAKE_CURRENT_BINARY_DIR}/${restore_tool}-env" gen-completion bash
  COMMAND "${CMAKE_CURRENT_BINARY_DIR}/${restore_tool}-env" gen-completion zsh
  COMMAND "${CMAKE_CURRENT_BINARY_DIR}/${restore_tool}-env" gen-completion fish
  COMMAND "${CMAKE_CURRENT_BINARY_DIR}/${restore_tool}-env" gen-man-pages "${restore_artifact_dir}/man"
  WORKING_DIRECTORY "${restore_artifact_dir}"
  DEPENDS ${restore_tool}
  COMMENT "Generating restore tool manual pages and shell completions"
  VERBATIM)

install(PROGRAMS "${CMAKE_CURRENT_BINARY_DIR}/${restore_tool}-env" DESTINATION ${CMAKE_INSTALL_BINDIR})
install(FILES "${restore_artifact_dir}/${restore_tool}.bash"
  DESTINATION ${CMAKE_INSTALL_DATADIR}/bash-completion/completions RENAME ${restore_tool})
install(FILES "${restore_artifact_dir}/${restore_tool}.zsh"
  DESTINATION ${CMAKE_INSTALL_DATADIR}/zsh/site-functions RENAME _${restore_tool})
install(FILES "${restore_artifact_dir}/${restore_tool}.fish"
  DESTINATION ${CMAKE_INSTALL_DATADIR}/fish/vendor_completions.d)
install(DIRECTORY "${restore_artifact_dir}/man/" DESTINATION ${CMAKE_INSTALL_MANDIR}/man1
  FILES_MATCHING PATTERN "*.1")
