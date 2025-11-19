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

find_package(Protobuf3 REQUIRED)

include_directories(${CMAKE_BINARY_DIR}/eos_cta ${PROTOBUF3_INCLUDE_DIRS})

set(FRONTEND_COMMON_SRC_FILES
  WorkflowEvent.cpp
  FrontendService.cpp
  ValidateToken.cpp
  AdminCmd.cpp
  RequestTracker.cpp
  AdminCmdOptions.cpp
  ActivityMountRuleLsResponseStream.cpp
  AdminLsResponseStream.cpp
  ArchiveRouteLsResponseStream.cpp
  DiskInstanceLsResponseStream.cpp
  DiskInstanceSpaceLsResponseStream.cpp
  DiskSystemLsResponseStream.cpp
  DriveLsResponseStream.cpp
  FailedRequestLsResponseStream.cpp
  GroupMountRuleLsResponseStream.cpp
  LogicalLibraryLsResponseStream.cpp
  MediaTypeLsResponseStream.cpp
  MountPolicyLsResponseStream.cpp
  PhysicalLibraryLsResponseStream.cpp
  RecycleTapeFileLsResponseStream.cpp
  RepackLsResponseStream.cpp
  RequesterMountRuleLsResponseStream.cpp
  ShowQueuesResponseStream.cpp
  StorageClassLsResponseStream.cpp
  TapeFileLsResponseStream.cpp
  TapeLsResponseStream.cpp
  TapePoolLsResponseStream.cpp
  VirtualOrganizationLsResponseStream.cpp
)

add_library(ctafrontendcommon STATIC ${FRONTEND_COMMON_SRC_FILES})

if(CTA_USE_PGSCHED)
    target_link_libraries(ctafrontendcommon PRIVATE ctacommon ctacatalogue)
else()
    target_link_libraries(ctafrontendcommon PRIVATE ctacommon ctaobjectstore ctacatalogue)
endif()

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
