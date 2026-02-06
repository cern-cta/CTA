# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

cmake_minimum_required (VERSION 3.17)

find_package(Protobuf3 REQUIRED)

include_directories(${CMAKE_BINARY_DIR}/eos_cta ${PROTOBUF3_INCLUDE_DIRS})

set(FRONTEND_COMMON_SRC_FILES
  WorkflowEvent.cpp
  FrontendService.cpp
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

set_property(TARGET ctafrontendcommonconfigunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctafrontendcommonconfigunittests PROPERTY   VERSION "${CTA_LIBVERSION}")
