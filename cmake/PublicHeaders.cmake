# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Attach every header under the calling directory's include/ to a target as a HEADERS file set.
# The file set adds include/ to the target's usage requirements and lets tooling (e.g. CMake Tools
# for IntelliSense) associate each header with the target's compile flags.
function(add_public_headers target)
  get_target_property(target_type ${target} TYPE)
  if(target_type STREQUAL "INTERFACE_LIBRARY")
    set(scope INTERFACE)
  else()
    set(scope PUBLIC)
  endif()

  set(include_dir "${CMAKE_CURRENT_SOURCE_DIR}/include")
  # CONFIGURE_DEPENDS re-runs the glob at build time, so newly added headers are picked up.
  file(GLOB_RECURSE headers CONFIGURE_DEPENDS "${include_dir}/*.hpp")

  target_sources(${target} ${scope}
    FILE_SET HEADERS
    BASE_DIRS "${include_dir}"
    FILES ${headers})
endfunction()
