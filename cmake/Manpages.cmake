# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

include(CMakeParseArguments)

# Man pages are built during normal builds, so Pandoc is a required build dependency.
find_program(PANDOC_EXECUTABLE NAMES pandoc REQUIRED)
mark_as_advanced(PANDOC_EXECUTABLE)

function(add_manpage name)
  set(one_value_arguments SOURCE)
  set(multi_value_arguments DEPENDS)
  cmake_parse_arguments(MANPAGE "" "${one_value_arguments}" "${multi_value_arguments}" ${ARGN})

  if(MANPAGE_UNPARSED_ARGUMENTS)
    message(FATAL_ERROR "Unknown arguments to add_manpage(${name}): ${MANPAGE_UNPARSED_ARGUMENTS}")
  endif()

  # By convention, each command keeps its Pandoc Markdown beside its CMakeLists.txt.
  if(NOT MANPAGE_SOURCE)
    set(MANPAGE_SOURCE "${name}.1cta.md")
  endif()

  # Absolute paths also allow callers to pass generated sources from the build tree.
  if(NOT IS_ABSOLUTE "${MANPAGE_SOURCE}")
    set(MANPAGE_SOURCE "${CMAKE_CURRENT_SOURCE_DIR}/${MANPAGE_SOURCE}")
  endif()

  set(output_file "${CMAKE_CURRENT_BINARY_DIR}/${name}.1")

  add_custom_command(
    OUTPUT "${output_file}"
    COMMAND "${PANDOC_EXECUTABLE}" --standalone --to man --output "${output_file}" "${MANPAGE_SOURCE}"
    DEPENDS "${MANPAGE_SOURCE}" ${MANPAGE_DEPENDS}
    COMMENT "Building ${name}(1) man page"
    VERBATIM
  )

  # Include the man page in the default build and expose a target for rebuilding it directly.
  add_custom_target(${name}_man_page ALL DEPENDS "${output_file}")
endfunction()
