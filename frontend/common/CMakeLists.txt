# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2015-2022 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.

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
include_directories(${CMAKE_SOURCE_DIR}/external/jwt-cpp/include/)

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
