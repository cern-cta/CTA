# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set(restore_tool cta-eos-restore-files)
set(restore_artifact_dir "${CMAKE_CURRENT_BINARY_DIR}/restore-tool-artifacts")
file(MAKE_DIRECTORY "${restore_artifact_dir}")

# Keep the installed multicall entry point relative to the installed executable.
file(CREATE_LINK "${restore_tool}" "${CMAKE_CURRENT_BINARY_DIR}/${restore_tool}-env" SYMBOLIC)

set(restore_artifacts
  "${restore_artifact_dir}/${restore_tool}.bash"
  "${restore_artifact_dir}/${restore_tool}.zsh"
  "${restore_artifact_dir}/${restore_tool}.fish"
  "${restore_artifact_dir}/man/${restore_tool}.1")

# Select the helper applet without assuming where Corrosion places the executable.
set(restore_env_command "${CMAKE_COMMAND}" -E env "OVERRIDE_BINARY_NAME=${restore_tool}-env"
  "$<TARGET_FILE:${restore_tool}>")
add_custom_command(OUTPUT ${restore_artifacts}
  COMMAND "${CMAKE_COMMAND}" -E rm -rf "${restore_artifact_dir}/man"
  COMMAND ${restore_env_command} gen-completion bash
  COMMAND ${restore_env_command} gen-completion zsh
  COMMAND ${restore_env_command} gen-completion fish
  COMMAND ${restore_env_command} gen-man-pages "${restore_artifact_dir}/man"
  WORKING_DIRECTORY "${restore_artifact_dir}"
  DEPENDS ${restore_tool} "$<TARGET_FILE:${restore_tool}>"
  COMMENT "Generating restore tool manual pages and shell completions"
  VERBATIM)
add_custom_target(cta-restore-tool-artifacts ALL DEPENDS ${restore_artifacts})

install(PROGRAMS "${CMAKE_CURRENT_BINARY_DIR}/${restore_tool}-env" DESTINATION ${CMAKE_INSTALL_BINDIR})
install(FILES "${restore_artifact_dir}/${restore_tool}.bash"
  DESTINATION ${CMAKE_INSTALL_DATADIR}/bash-completion/completions RENAME ${restore_tool})
install(FILES "${restore_artifact_dir}/${restore_tool}.zsh"
  DESTINATION ${CMAKE_INSTALL_DATADIR}/zsh/site-functions RENAME _${restore_tool})
install(FILES "${restore_artifact_dir}/${restore_tool}.fish"
  DESTINATION ${CMAKE_INSTALL_DATADIR}/fish/vendor_completions.d)
install(DIRECTORY "${restore_artifact_dir}/man/" DESTINATION ${CMAKE_INSTALL_MANDIR}/man1
  FILES_MATCHING PATTERN "*.1")
