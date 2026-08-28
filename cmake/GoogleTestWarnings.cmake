# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# EL9's GoogleTest 1.11 leaves matcher storage uninitialized, triggering GCC 14+ diagnostics.
# Suppress these warnings on EL9; other platforms retain their normal warning policy.
# Remove when EL9 provides a fixed GoogleTest.
if(NOT PLATFORM STREQUAL "el9" OR NOT CMAKE_CXX_COMPILER_ID STREQUAL "GNU"
   OR CMAKE_CXX_COMPILER_VERSION VERSION_LESS 14)
  return()
endif()

# CTA test libraries end in unittests; test runners and hardware test tools use the cta-*-test prefixes.
# PRIVATE options ensure production targets retain their normal warning policy.
function(cta_relax_googletest_warnings directory)
  get_property(targets DIRECTORY "${directory}" PROPERTY BUILDSYSTEM_TARGETS)
  foreach(target IN LISTS targets)
    get_target_property(target_type ${target} TYPE)
    if(target MATCHES "unittests$|^cta-unit-test|^cta-test-"
       AND target_type MATCHES "^(EXECUTABLE|STATIC_LIBRARY|SHARED_LIBRARY|MODULE_LIBRARY|OBJECT_LIBRARY)$")
      target_compile_options(${target} PRIVATE
        "$<$<COMPILE_LANGUAGE:CXX>:-Wno-uninitialized;-Wno-maybe-uninitialized>")
    endif()
  endforeach()

  get_property(subdirectories DIRECTORY "${directory}" PROPERTY SUBDIRECTORIES)
  foreach(subdirectory IN LISTS subdirectories)
    cta_relax_googletest_warnings("${subdirectory}")
  endforeach()
endfunction()

cta_relax_googletest_warnings("${PROJECT_SOURCE_DIR}")

# Wrapper.cpp defines gMock wrappers alongside the real system wrappers in a production library.
# Limit the exception to that translation unit rather than relaxing the whole library.
set_property(SOURCE "${PROJECT_SOURCE_DIR}/taped/system/Wrapper.cpp"
  DIRECTORY "${PROJECT_SOURCE_DIR}/taped/system"
  APPEND PROPERTY COMPILE_OPTIONS -Wno-uninitialized -Wno-maybe-uninitialized)
