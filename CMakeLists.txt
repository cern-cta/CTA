# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required(VERSION 3.19)

project(cta)

# -----------------------------------------------------------------------------
# Project metadata
# -----------------------------------------------------------------------------
include(cmake/LoadProjectJson.cmake)

# -----------------------------------------------------------------------------
# User-configurable options
# -----------------------------------------------------------------------------
set(CTA_USE_PGSCHED FALSE CACHE BOOL "Build with the postgres scheduler")
option(DISABLE_ORACLE_SUPPORT "Build without Oracle catalogue support" OFF)
option(ENABLE_ADDRESS_SANITIZER "Build with AddressSanitizer" OFF)
option(ENABLE_STDOUT_LOGGING "Enable stdout logging for tests" OFF)

if(NOT DEFINED SKIP_UNIT_TESTS)
  set(SKIP_UNIT_TESTS 0)
endif()
if(NOT DEFINED SKIP_DEBUG_PACKAGES)
  set(SKIP_DEBUG_PACKAGES 0)
endif()
if(NOT DEFINED ENABLE_CCACHE)
  set(ENABLE_CCACHE 0)
endif()

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "${PROJECT_DEFAULT_BUILD_TYPE}" CACHE STRING "Choose the type of build." FORCE)
else()
  set(CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE}" CACHE STRING "Choose the type of build.")
endif()
set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS Debug Release MinSizeRel RelWithDebInfo)

# PackageOnly intentionally retains its historical DEFINED semantics.
if(DEFINED PackageOnly)
  set(CTA_CONFIGURATION_MODE "package-only")
  set(COMPILE_PACKAGING 1)
else()
  set(CTA_CONFIGURATION_MODE "full")
  if(NOT DEFINED COMPILE_PACKAGING)
    set(COMPILE_PACKAGING 1)
  endif()
endif()

if(NOT DEFINED JOBS_COUNT)
  include(ProcessorCount)
  ProcessorCount(PROCESSOR_COUNT)
  set(JOBS_COUNT ${PROCESSOR_COUNT})
endif()

# -----------------------------------------------------------------------------
# Derived configuration
# -----------------------------------------------------------------------------
set(OCCI_SUPPORT ON)
if(DISABLE_ORACLE_SUPPORT)
  set(OCCI_SUPPORT OFF)
  set(RPM_ORACLE "-DDISABLE_ORACLE_SUPPORT:BOOL=ON" CACHE STRING "" FORCE)
  set(RPM_USING_ORACLE "0" CACHE STRING "" FORCE)
else()
  set(RPM_ORACLE "%{nil}" CACHE STRING "" FORCE)
  set(RPM_USING_ORACLE "1" CACHE STRING "" FORCE)
endif()

if(CTA_USE_PGSCHED)
  set(CTA_SCHEDULER_BACKEND "PostgreSQL")
  set(RPM_USINGOBJECTSTORE "0" CACHE STRING "" FORCE)
  set(RPM_SCHEDOPT "-DCTA_USE_PGSCHED:Bool=TRUE" CACHE STRING "" FORCE)
else()
  set(CTA_SCHEDULER_BACKEND "object store")
  set(RPM_USINGOBJECTSTORE "1" CACHE STRING "" FORCE)
  set(RPM_SCHEDOPT "-DCTA_USE_PGSCHED:Bool=FALSE" CACHE STRING "" FORCE)
endif()

if(ENABLE_ADDRESS_SANITIZER)
  set(RPM_ADDRESS_SANITIZER_OPT "-DENABLE_ADDRESS_SANITIZER:Bool=TRUE" CACHE STRING "" FORCE)
else()
  set(RPM_ADDRESS_SANITIZER_OPT "-DENABLE_ADDRESS_SANITIZER:Bool=FALSE" CACHE STRING "" FORCE)
endif()

# -----------------------------------------------------------------------------
# Compiler policy
# -----------------------------------------------------------------------------

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_compile_options(
  "$<$<COMPILE_LANGUAGE:C>:-fPIC;-Wall;-Wextra;-Werror;-Wno-unused-parameter>"
  "$<$<COMPILE_LANGUAGE:CXX>:-fPIC;-pedantic;-Wall;-Wextra;-Werror;-Wno-unused-parameter;-fstack-protector-all;-std=${PROJECT_CPP_VERSION}>"
)

if(ENABLE_ADDRESS_SANITIZER)
  add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address -fno-omit-frame-pointer)
endif()

# To enable logging for testing purposes set -DENABLE_STDOUT_LOGGING=1
if(ENABLE_STDOUT_LOGGING)
  add_compile_definitions(STDOUT_LOGGING)
endif()

if(CTA_USE_PGSCHED)
  add_compile_definitions(CTA_PGSCHED)
endif()

# Create the version target after applying the project compiler policy.
include(cmake/CTAVersions.cmake)

# -----------------------------------------------------------------------------
# Full build
# -----------------------------------------------------------------------------
if(NOT DEFINED PackageOnly)
  set(CMAKE_DISABLE_SOURCE_CHANGES ON)
  list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")
  include(GNUInstallDirs)
  set(CMAKE_INSTALL_PREFIX "/")
  set(CTA_SYSTEMD_UNIT_DIR "usr/lib/systemd/system" CACHE PATH
    "Installation directory for systemd system units")

  include_directories(${PROJECT_SOURCE_DIR})
  include_directories(${CMAKE_BINARY_DIR})

  # ---------------------------------------------------------------------------
  # Dependencies
  # ---------------------------------------------------------------------------
  message(STATUS "---------------------------------------------")
  message(STATUS "CTA dependency discovery")
  message(STATUS "---------------------------------------------")
  find_package(Protobuf3 REQUIRED)
  find_package(opentelemetry-cpp CONFIG REQUIRED)
  find_package(GRPC REQUIRED)
  find_package(xrootd REQUIRED)
  find_package(xrootdclient REQUIRED)
  find_package(postgres REQUIRED)

  if(OCCI_SUPPORT)
    find_package(oracle-instantclient REQUIRED)
  endif()

  if(NOT CTA_USE_PGSCHED)
    find_package(librados2 REQUIRED)
  endif()

  install (CODE "message (STATUS \"Installing directory \$ENV{DESTDIR}/var/log/cta\")")
  install (CODE "file (MAKE_DIRECTORY \$ENV{DESTDIR}/var/log/cta)")

  #
  # Location of git submodule containing the XRootD SSI Protocol Buffer bindings
  #
  set(XRD_SSI_PB_DIR ${PROJECT_SOURCE_DIR}/xrootd-ssi-protobuf-interface)

  # ---------------------------------------------------------------------------
  # Components
  # ---------------------------------------------------------------------------
  # Pandocology (to create man pages from Markdown)
  include(cmake/Pandocology.cmake)
  add_subdirectory(eos_cta)
  add_subdirectory(eos_grpc_client)
  add_subdirectory(xroot_plugins)
  add_subdirectory(frontend/grpc)
  add_subdirectory(frontend/common)
  add_subdirectory(catalogue)
  add_subdirectory(common)
  add_subdirectory(disk)
  add_subdirectory(mediachanger)

  if(NOT CTA_USE_PGSCHED)
    add_subdirectory(objectstore)
  endif()

  add_subdirectory(rdbms)
  add_subdirectory(scheduler)
  add_subdirectory(taped)
  add_subdirectory(maintd)
  add_subdirectory(tools)
  add_subdirectory(tests)

  if(TARGET ctaobjectstore-protobuf-generation)
    function(cta_add_objectstore_protobuf_dependency directory)
      get_property(directory_targets DIRECTORY "${directory}" PROPERTY BUILDSYSTEM_TARGETS)
      foreach(target IN LISTS directory_targets)
        get_target_property(target_type "${target}" TYPE)
        if(target_type MATCHES "^(EXECUTABLE|STATIC_LIBRARY|SHARED_LIBRARY|MODULE_LIBRARY|OBJECT_LIBRARY)$")
          add_dependencies("${target}" ctaobjectstore-protobuf-generation)
        endif()
      endforeach()

      get_property(subdirectories DIRECTORY "${directory}" PROPERTY SUBDIRECTORIES)
      foreach(subdirectory IN LISTS subdirectories)
        cta_add_objectstore_protobuf_dependency("${subdirectory}")
      endforeach()
    endfunction()
    cta_add_objectstore_protobuf_dependency("${CMAKE_CURRENT_SOURCE_DIR}")
  endif()

endif()

# -----------------------------------------------------------------------------
# Packaging
# -----------------------------------------------------------------------------
if(COMPILE_PACKAGING STREQUAL "1")
  include(cmake/UseRPMToolsEnvironment.cmake)
  set(CPACK_SOURCE_PACKAGE_FILE_NAME
   "${PROJECT_NAME}-${CTA_VERSION}-${CTA_RELEASE}")
  # Note that not ignoring build folders within the repository here will cause cpack to take longer and longer
  # as it is trying to pack the build directory as well
  set(CPACK_SOURCE_IGNORE_FILES
    # Ignore any file or directory starting with a dot
    # This is important for e.g. the caching directories in the gitlab CI
    "/[.]"
    "/ci/system_tests"
    "/ci/orchestration"
    "/build_srpm/"
    "/build_rpm/"
    "/image_rpms/"
    "/assets/")
  include(CPack)
  include(cmake/UseRPMTools.cmake)
  if (RPMTools_FOUND)
    RPMTools_ADD_RPM_TARGETS(
      ${PROJECT_NAME} ${PROJECT_NAME}.spec.in)
  endif()
endif()

# -----------------------------------------------------------------------------
# Configuration summary
# -----------------------------------------------------------------------------
message(STATUS "---------------------------------------------")
message(STATUS "CTA configuration summary")
message(STATUS "---------------------------------------------")
message(STATUS "CTA_VERSION: ${CTA_VERSION}-${CTA_RELEASE}")
message(STATUS "CTA_LIBVERSION: ${CTA_LIBVERSION} (SOVERSION ${CTA_SOVERSION})")
message(STATUS "SUPPORTED_CATALOGUE_VERSIONS: ${SUPPORTED_CTA_CATALOGUE_SCHEMA_VERSIONS_JOINED}")
message(STATUS "CONFIGURATION_MODE: ${CTA_CONFIGURATION_MODE}")
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
message(STATUS "CXX_STANDARD: ${PROJECT_CPP_VERSION}")
message(STATUS "SCHEDULER_BACKEND: ${CTA_SCHEDULER_BACKEND}")
message(STATUS "ORACLE_SUPPORT: ${OCCI_SUPPORT}")
message(STATUS "ENABLE_ADDRESS_SANITIZER: ${ENABLE_ADDRESS_SANITIZER}")
message(STATUS "ENABLE_STDOUT_LOGGING: ${ENABLE_STDOUT_LOGGING}")
message(STATUS "COMPILE_PACKAGING: ${COMPILE_PACKAGING}")
message(STATUS "SKIP_UNIT_TESTS: ${SKIP_UNIT_TESTS}")
message(STATUS "SKIP_DEBUG_PACKAGES: ${SKIP_DEBUG_PACKAGES}")
message(STATUS "ENABLE_CCACHE: ${ENABLE_CCACHE}")
message(STATUS "CMAKE_INSTALL_PREFIX: ${CMAKE_INSTALL_PREFIX}")
message(STATUS "CTA_SYSTEMD_UNIT_DIR: ${CTA_SYSTEMD_UNIT_DIR}")
message(STATUS "CMAKE_GENERATOR: ${CMAKE_GENERATOR}")
message(STATUS "JOBS_COUNT: ${JOBS_COUNT}")
