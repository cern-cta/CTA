# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

cmake_minimum_required (VERSION 3.17)

set (FRONTEND_COMMON_CONFIG_UNIT_TESTS_LIB_SRC_FILES
  ../../common/config/Config.cpp
  ../../common/config/ConfigGetOptionValueUIntTests.cpp
)

add_library (ctafrontendcommonconfigunittests SHARED
  ${FRONTEND_COMMON_CONFIG_UNIT_TESTS_LIB_SRC_FILES})
set_property(TARGET ctafrontendcommonconfigunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctafrontendcommonconfigunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

install(TARGETS ctafrontendcommonconfigunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

## add a library for the unit tests I am adding for token validation
include_directories(${CMAKE_SOURCE_DIR}/common/jwt-cpp/include/)

set (FRONTEND_COMMON_TOKEN_VALIDATION_UNIT_TESTS_LIB_SRC_FILES
  ValidateTokenTests.cpp
  ValidateToken.cpp
)

add_library(ctafrontendvalidatetokenunittests SHARED
  ${FRONTEND_COMMON_TOKEN_VALIDATION_UNIT_TESTS_LIB_SRC_FILES}
)

set_property(TARGET ctafrontendcommonconfigunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctafrontendcommonconfigunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries(ctafrontendvalidatetokenunittests JwkCache)
install(TARGETS ctafrontendvalidatetokenunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
