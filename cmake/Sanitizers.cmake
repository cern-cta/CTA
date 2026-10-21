# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set(CTA_SANITIZERS "none" CACHE STRING "Sanitizers: none, address, undefined, address,undefined, or thread")
set_property(CACHE CTA_SANITIZERS PROPERTY STRINGS none address undefined "address,undefined" thread)

set(_cta_sanitizer_modes none address undefined "address,undefined" thread)
if(NOT CTA_SANITIZERS IN_LIST _cta_sanitizer_modes)
  message(FATAL_ERROR "Invalid CTA_SANITIZERS: ${CTA_SANITIZERS}. Choose none, address, undefined, address,undefined, or thread")
endif()

# Apply instrumentation to both C/C++ compilation and executable/shared-library linking.
function(cta_enable_sanitizers)
  if(CTA_SANITIZERS STREQUAL "none")
    return()
  endif()
  if(NOT CMAKE_C_COMPILER_ID MATCHES "^(GNU|Clang)$" OR NOT CMAKE_CXX_COMPILER_ID MATCHES "^(GNU|Clang)$")
    message(FATAL_ERROR "CTA sanitizers require GCC or Clang")
  endif()
  add_compile_options(-fsanitize=${CTA_SANITIZERS} -fno-omit-frame-pointer)
  add_link_options(-fsanitize=${CTA_SANITIZERS} -fno-omit-frame-pointer)

  if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # GCC reports optional/variant cleanup as possibly uninitialized with sanitizer instrumentation.
    add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-Wno-error=maybe-uninitialized>)
  endif()

  if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CTA_SANITIZERS MATCHES "(^|,)address(,|$)")
    # GCC LTO makes inline standard-library globals trigger false ASan ODR violations across shared libraries.
    add_compile_options(-fno-lto)
    add_link_options(-fno-lto)
  endif()

  if(CTA_SANITIZERS MATCHES "(^|,)undefined(,|$)")
    # Make UBSan findings fatal regardless of the runtime environment.
    add_compile_options(-fno-sanitize-recover=undefined)
  endif()
endfunction()
