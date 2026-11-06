# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Attach headers under the calling directory's include/ to a target as a HEADERS file set.
# The file set adds include/ to the target's usage requirements and lets tooling (e.g. CMake Tools
# for IntelliSense) associate each header with the target's compile flags.
#
#   add_public_headers(<target> [PATTERNS <glob>...])
#
# PATTERNS are recursive globs relative to include/; they select the headers belonging to <target>
# when several targets share one include/ directory. By default every C/C++ header is attached.
function(add_public_headers target)
  cmake_parse_arguments(PARSE_ARGV 1 arg "" "" "PATTERNS")
  if(NOT arg_PATTERNS)
    set(arg_PATTERNS "*.hpp" "*.h")
  endif()

  get_target_property(target_type ${target} TYPE)
  if(target_type STREQUAL "INTERFACE_LIBRARY")
    set(scope INTERFACE)
  else()
    set(scope PUBLIC)
  endif()

  set(include_dir "${CMAKE_CURRENT_SOURCE_DIR}/include")
  list(TRANSFORM arg_PATTERNS PREPEND "${include_dir}/")
  # CONFIGURE_DEPENDS re-runs the glob at build time, so newly added headers are picked up.
  file(GLOB_RECURSE headers CONFIGURE_DEPENDS ${arg_PATTERNS})
  if(NOT headers)
    message(FATAL_ERROR "add_public_headers(${target}): no headers match ${arg_PATTERNS}")
  endif()

  target_sources(${target} ${scope}
    FILE_SET HEADERS
    BASE_DIRS "${include_dir}"
    FILES ${headers})
endfunction()
