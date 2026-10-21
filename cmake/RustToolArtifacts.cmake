# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Tools expose gen-completion and gen-man-pages through their <tool>-env applet.
function(cta_add_rust_tool_artifacts tool)
  set(artifact_dir "${CMAKE_CURRENT_BINARY_DIR}/${tool}-artifacts")
  file(MAKE_DIRECTORY "${artifact_dir}")

  # Keep the installed multicall entry point relative to the installed executable.
  file(CREATE_LINK "${tool}" "${CMAKE_CURRENT_BINARY_DIR}/${tool}-env" SYMBOLIC)

  set(artifacts
    "${artifact_dir}/${tool}.bash"
    "${artifact_dir}/${tool}.zsh"
    "${artifact_dir}/${tool}.fish"
    "${artifact_dir}/man/${tool}.1")

  # Select the helper applet without assuming where Corrosion places the executable.
  set(env_command "${CMAKE_COMMAND}" -E env "OVERRIDE_BINARY_NAME=${tool}-env"
    "$<TARGET_FILE:${tool}>")
  add_custom_command(OUTPUT ${artifacts}
    COMMAND "${CMAKE_COMMAND}" -E rm -rf "${artifact_dir}/man"
    COMMAND ${env_command} gen-completion bash
    COMMAND ${env_command} gen-completion zsh
    COMMAND ${env_command} gen-completion fish
    COMMAND ${env_command} gen-man-pages "${artifact_dir}/man"
    WORKING_DIRECTORY "${artifact_dir}"
    DEPENDS ${tool} "$<TARGET_FILE:${tool}>"
    COMMENT "Generating ${tool} manual pages and shell completions"
    VERBATIM)
  add_custom_target(${tool}-artifacts ALL DEPENDS ${artifacts})

  install(PROGRAMS "${CMAKE_CURRENT_BINARY_DIR}/${tool}-env" DESTINATION ${CMAKE_INSTALL_BINDIR})
  install(FILES "${artifact_dir}/${tool}.bash"
    DESTINATION ${CMAKE_INSTALL_DATADIR}/bash-completion/completions RENAME ${tool})
  install(FILES "${artifact_dir}/${tool}.zsh"
    DESTINATION ${CMAKE_INSTALL_DATADIR}/zsh/site-functions RENAME _${tool})
  install(FILES "${artifact_dir}/${tool}.fish"
    DESTINATION ${CMAKE_INSTALL_DATADIR}/fish/vendor_completions.d)
  install(DIRECTORY "${artifact_dir}/man/" DESTINATION ${CMAKE_INSTALL_MANDIR}/man1
    FILES_MATCHING PATTERN "*.1")
endfunction()
