# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required(VERSION 3.19)

project(cta)

# -----------------------------------------------------------------------------
# Project metadata
# -----------------------------------------------------------------------------
include(cmake/DetectPlatform.cmake)

if(CTA_PACKAGE_FORMAT STREQUAL "unknown")
  message(FATAL_ERROR
    "CTA recognizes ${CTA_OS_ID} ${CTA_OS_VERSION} as a ${CTA_OS_FAMILY} platform "
    "(${PLATFORM}), but this platform is not supported yet")
endif()

include(cmake/LoadProjectJson.cmake)

# -----------------------------------------------------------------------------
# User-configurable options
# -----------------------------------------------------------------------------
option(CTA_USE_PGSCHED "Build with the PostgreSQL scheduler" OFF)
set(CTA_USE_EXTRA_TELEMETRY FALSE CACHE BOOL
  "Build with telemetry instrumentation for postgres performance measurements")
option(CTA_WITH_ORACLE "Build with Oracle catalogue support" ON)
option(ENABLE_ADDRESS_SANITIZER "Build with AddressSanitizer" OFF)
option(ENABLE_CCACHE "Use ccache for compilation" OFF)
option(CTA_ENABLE_TEST_STDOUT_LOGGING "Enable stdout logging in tests" OFF)
option(CTA_SHOW_CONFIGURATION_SUMMARY "Show the configuration summary" ON)
set(CTA_PACKAGE_MODE "binary" CACHE STRING
  "Package mode: none, source, or binary")
set_property(CACHE CTA_PACKAGE_MODE PROPERTY STRINGS none source binary)
option(CTA_RUN_UNIT_TESTS "Run unit tests while building packages" ON)
option(CTA_BUILD_DEBUG_PACKAGES "Build native debug packages" ON)

set(BUILD_CMDLINE_ONLY FALSE)

# Applications using the runtime library add their example configuration checks as dependencies of this target.
add_custom_target(cta-example-config-checks)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "${PROJECT_DEFAULT_BUILD_TYPE}" CACHE STRING "Choose the type of build." FORCE)
else()
  set(CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE}" CACHE STRING "Choose the type of build.")
endif()
set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS Debug Release MinSizeRel RelWithDebInfo)

set(CTA_PACKAGE_MODES none source binary)
if(NOT CTA_PACKAGE_MODE IN_LIST CTA_PACKAGE_MODES)
  message(FATAL_ERROR
    "CTA_PACKAGE_MODE must be one of: none, source, binary (got: ${CTA_PACKAGE_MODE})")
endif()

if(CTA_PACKAGE_MODE STREQUAL "source")
  set(CTA_CONFIGURATION_MODE "package-only")
else()
  set(CTA_CONFIGURATION_MODE "full")
endif()

if(NOT DEFINED JOBS_COUNT)
  include(ProcessorCount)
  ProcessorCount(JOBS_COUNT)
endif()
set(JOBS_COUNT "${JOBS_COUNT}" CACHE STRING "Maximum number of parallel build jobs")

# -----------------------------------------------------------------------------
# Derived configuration
# -----------------------------------------------------------------------------
if(CTA_RUN_UNIT_TESTS)
  set(PACKAGE_RUN_UNIT_TESTS 1)
else()
  set(PACKAGE_RUN_UNIT_TESTS 0)
endif()

if(CTA_BUILD_DEBUG_PACKAGES)
  set(PACKAGE_BUILD_DEBUG_PACKAGES 1)
else()
  set(PACKAGE_BUILD_DEBUG_PACKAGES 0)
endif()

if(CTA_WITH_ORACLE)
  set(PACKAGE_WITH_ORACLE 1)
else()
  set(PACKAGE_WITH_ORACLE 0)
endif()

if(CTA_USE_PGSCHED)
  set(PACKAGE_USE_PGSCHED 1)
else()
  set(PACKAGE_USE_PGSCHED 0)
endif()

# -----------------------------------------------------------------------------
# Compiler policy
# -----------------------------------------------------------------------------

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(ENABLE_CCACHE)
  find_program(CTA_CCACHE_EXECUTABLE NAMES ccache REQUIRED)
  set(CMAKE_C_COMPILER_LAUNCHER "${CTA_CCACHE_EXECUTABLE}")
  set(CMAKE_CXX_COMPILER_LAUNCHER "${CTA_CCACHE_EXECUTABLE}")
endif()

add_compile_options(
  "$<$<COMPILE_LANGUAGE:C>:-fPIC;-Wall;-Wextra;-Werror;-Wno-unused-parameter>"
  "$<$<COMPILE_LANGUAGE:CXX>:-fPIC;-pedantic;-Wall;-Wextra;-Werror;-Wno-unused-parameter;-fstack-protector-all;-std=${PROJECT_CPP_VERSION}>"
)

if(ENABLE_ADDRESS_SANITIZER)
  add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address -fno-omit-frame-pointer)
endif()

# This definition is only used by test logging implementations.
if(CTA_ENABLE_TEST_STDOUT_LOGGING)
  add_compile_definitions(STDOUT_LOGGING)
endif()

if(CTA_USE_PGSCHED)
  add_compile_definitions(CTA_PGSCHED)
endif()

if(CTA_USE_EXTRA_TELEMETRY)
  add_compile_definitions(CTA_USE_EXTRA_TELEMETRY)
endif()

# Create the version target after applying the project compiler policy.
include(cmake/CTAVersions.cmake)

# -----------------------------------------------------------------------------
# Full build
# -----------------------------------------------------------------------------
if(NOT CTA_PACKAGE_MODE STREQUAL "source")
  set(CMAKE_DISABLE_SOURCE_CHANGES ON)
  list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")
  include(GNUInstallDirs)
  if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    set(CMAKE_INSTALL_PREFIX "/usr" CACHE PATH "Installation prefix" FORCE)
  endif()
  set(CTA_SYSTEMD_UNIT_DIR "lib/systemd/system" CACHE PATH
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
  find_package(xrootdclient REQUIRED)
  find_package(postgres REQUIRED)

  if(CTA_WITH_ORACLE)
    find_package(oracle-instantclient REQUIRED)
  endif()

  if(NOT CTA_USE_PGSCHED)
    find_package(librados2 REQUIRED)
  endif()

  #
  # Location of git submodule containing the XRootD SSI Protocol Buffer bindings
  #
  set(XRD_SSI_PB_DIR ${PROJECT_SOURCE_DIR}/lib/protobuf/external/xrootd-ssi-protobuf-interface)

  # ---------------------------------------------------------------------------
  # Components
  # ---------------------------------------------------------------------------
  include(cmake/Manpages.cmake)
  add_subdirectory(packaging)
  add_subdirectory(lib/plugin-manager)
  add_subdirectory(lib/protobuf)
  add_subdirectory(common)
  add_subdirectory(lib/telemetry)
  add_subdirectory(lib/runtime)
  add_subdirectory(frontend/grpc)
  add_subdirectory(frontend/common)
  add_subdirectory(catalogue)
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

endif()

# -----------------------------------------------------------------------------
# Packaging
# -----------------------------------------------------------------------------
if(NOT CTA_PACKAGE_MODE STREQUAL "none")
  set(CPACK_SOURCE_PACKAGE_FILE_NAME
   "${PROJECT_NAME}-${CTA_VERSION}-${CTA_RELEASE}")
  # Note that not ignoring build folders within the repository here will cause cpack to take longer and longer
  # as it is trying to pack the build directory as well
  set(CPACK_SOURCE_IGNORE_FILES
    # Ignore any file or directory starting with a dot
    # This is important for e.g. the caching directories in the gitlab CI
    "/[.]"
    "${CMAKE_BINARY_DIR}/"
    "/ci/system_tests"
    "/ci/orchestration"
    "/build/"
    # Keep excluding build directories created by the superseded packaging scripts
    "/build_srpm/"
    "/build_rpm/"
    "/image_rpms/"
    "/pod_logs/"
    "/tmp/"
    "[.]core$"
    "/assets/")
  include(CPack)

  if(CTA_PACKAGE_FORMAT STREQUAL "rpm")
    include(cmake/RPMTools.cmake)
    rpmtools_add_rpm_targets(
      "${PROJECT_NAME}"
      "packaging/rpm/${PROJECT_NAME}.spec.in"
      "${CTA_PACKAGE_MODE}"
      "packaging/sysusers/cta.conf")
  elseif(CTA_PACKAGE_FORMAT STREQUAL "deb")
    include(cmake/DebTools.cmake)
  else()
    message(FATAL_ERROR "Unsupported package format: ${CTA_PACKAGE_FORMAT}")
  endif()
endif()

# -----------------------------------------------------------------------------
# Configuration summary
# -----------------------------------------------------------------------------
if(CTA_SHOW_CONFIGURATION_SUMMARY)
  message(STATUS "---------------------------------------------")
  message(STATUS "CTA configuration summary")
  message(STATUS "---------------------------------------------")
  message(STATUS "Project")
  message(STATUS "CTA_VERSION: ${CTA_VERSION}-${CTA_RELEASE}")
  message(STATUS "CTA_LIBVERSION: ${CTA_LIBVERSION} (SOVERSION ${CTA_SOVERSION})")
  message(STATUS "SUPPORTED_CATALOGUE_VERSIONS: ${SUPPORTED_CTA_CATALOGUE_VERSIONS_JOINED}")
  message(STATUS "")
  message(STATUS "Build")
  message(STATUS "PLATFORM: ${PLATFORM} (${CTA_OS_ID} ${CTA_OS_VERSION}, ${CTA_OS_FAMILY}, ${CTA_PACKAGE_FORMAT})")
  message(STATUS "CMAKE_VERSION: ${CMAKE_VERSION}")
  message(STATUS "CMAKE_SYSTEM_PROCESSOR: ${CMAKE_SYSTEM_PROCESSOR}")
  message(STATUS "CXX_COMPILER: ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
  message(STATUS "CXX_STANDARD: ${PROJECT_CPP_VERSION}")
  message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
  message(STATUS "CMAKE_GENERATOR: ${CMAKE_GENERATOR}")
  message(STATUS "CMAKE_TOOLCHAIN_FILE: ${CMAKE_TOOLCHAIN_FILE}")
  message(STATUS "JOBS_COUNT: ${JOBS_COUNT}")
  message(STATUS "CONFIGURATION_MODE: ${CTA_CONFIGURATION_MODE}")
  message(STATUS "ENABLE_ADDRESS_SANITIZER: ${ENABLE_ADDRESS_SANITIZER}")
  message(STATUS "ENABLE_CCACHE: ${ENABLE_CCACHE}")
  message(STATUS "")
  message(STATUS "Features")
  message(STATUS "CTA_USE_PGSCHED: ${CTA_USE_PGSCHED}")
  message(STATUS "CTA_WITH_ORACLE: ${CTA_WITH_ORACLE}")
  message(STATUS "CTA_USE_EXTRA_TELEMETRY: ${CTA_USE_EXTRA_TELEMETRY}")
  message(STATUS "")
  message(STATUS "Testing")
  message(STATUS "CTA_ENABLE_TEST_STDOUT_LOGGING: ${CTA_ENABLE_TEST_STDOUT_LOGGING}")
  message(STATUS "")
  message(STATUS "Packaging")
  message(STATUS "CTA_PACKAGE_MODE: ${CTA_PACKAGE_MODE}")
  message(STATUS "CTA_RUN_UNIT_TESTS: ${CTA_RUN_UNIT_TESTS}")
  message(STATUS "CTA_BUILD_DEBUG_PACKAGES: ${CTA_BUILD_DEBUG_PACKAGES}")
  message(STATUS "")
  message(STATUS "Installation")
  message(STATUS "CMAKE_INSTALL_PREFIX: ${CMAKE_INSTALL_PREFIX}")
  message(STATUS "CTA_SYSTEMD_UNIT_DIR: ${CTA_SYSTEMD_UNIT_DIR}")
  message(STATUS "")
endif()
