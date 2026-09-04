# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required(VERSION 3.19)

project(cta)

# -----------------------------------------------------------------------------
# Project metadata
# -----------------------------------------------------------------------------
include(cmake/DetectPlatform.cmake)

if(NOT CTA_PLATFORM_SUPPORTED)
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
option(CTA_RPM_SOURCE_PACKAGE_ONLY "Configure only the source-package targets" OFF)
option(CTA_RPM_ENABLE_TARGETS "Create RPM and SRPM build targets" ON)
option(CTA_RPM_RUN_UNIT_TESTS "Run unit tests while building RPMs" ON)
option(CTA_RPM_BUILD_DEBUG_PACKAGES "Build RPM debug packages" ON)

set(BUILD_CMDLINE_ONLY FALSE)

# Applications using the runtime library add their example configuration checks as dependencies of this target.
add_custom_target(cta-example-config-checks)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "${PROJECT_DEFAULT_BUILD_TYPE}" CACHE STRING "Choose the type of build." FORCE)
else()
  set(CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE}" CACHE STRING "Choose the type of build.")
endif()
set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS Debug Release MinSizeRel RelWithDebInfo)

if(CTA_RPM_SOURCE_PACKAGE_ONLY)
  set(CTA_CONFIGURATION_MODE "package-only")
  set(CTA_RPM_ENABLE_TARGETS ON)
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
if(CTA_RPM_RUN_UNIT_TESTS)
  set(RPM_SKIP_UNIT_TESTS 0)
else()
  set(RPM_SKIP_UNIT_TESTS 1)
endif()

if(CTA_RPM_BUILD_DEBUG_PACKAGES)
  set(RPM_SKIP_DEBUG_PACKAGES 0)
else()
  set(RPM_SKIP_DEBUG_PACKAGES 1)
endif()

set(OCCI_SUPPORT "${CTA_WITH_ORACLE}")
if(NOT CTA_WITH_ORACLE)
  set(OCCI_SUPPORT OFF)
  set(RPM_ORACLE "-DCTA_WITH_ORACLE:BOOL=OFF" CACHE STRING "" FORCE)
  set(RPM_USING_ORACLE "0" CACHE STRING "" FORCE)
else()
  set(RPM_ORACLE "%{nil}" CACHE STRING "" FORCE)
  set(RPM_USING_ORACLE "1" CACHE STRING "" FORCE)
endif()

if(CTA_USE_PGSCHED)
  set(CTA_SCHEDULER_BACKEND "Postgres")
  set(RPM_USINGOBJECTSTORE "0" CACHE STRING "" FORCE)
  set(RPM_SCHEDOPT "-DCTA_USE_PGSCHED:Bool=TRUE" CACHE STRING "" FORCE)
else()
  set(CTA_SCHEDULER_BACKEND "objectstore")
  set(RPM_USINGOBJECTSTORE "1" CACHE STRING "" FORCE)
  set(RPM_SCHEDOPT "-DCTA_USE_PGSCHED:Bool=FALSE" CACHE STRING "" FORCE)
endif()

if(ENABLE_ADDRESS_SANITIZER)
  set(RPM_ADDRESS_SANITIZER_OPT "-DENABLE_ADDRESS_SANITIZER:BOOL=ON" CACHE STRING "" FORCE)
else()
  set(RPM_ADDRESS_SANITIZER_OPT "-DENABLE_ADDRESS_SANITIZER:BOOL=OFF" CACHE STRING "" FORCE)
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
  set(RPM_PERF_TELEMETRY_OPT "-DCTA_USE_EXTRA_TELEMETRY:Bool=TRUE" CACHE STRING "" FORCE)
else()
  set(RPM_PERF_TELEMETRY_OPT "-DCTA_USE_EXTRA_TELEMETRY:Bool=FALSE" CACHE STRING "" FORCE)
endif()

# Create the version target after applying the project compiler policy.
include(cmake/CTAVersions.cmake)

# -----------------------------------------------------------------------------
# Full build
# -----------------------------------------------------------------------------
if(NOT CTA_RPM_SOURCE_PACKAGE_ONLY)
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

endif()

# -----------------------------------------------------------------------------
# Packaging
# -----------------------------------------------------------------------------
if(CTA_RPM_ENABLE_TARGETS)
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
  include(cmake/RPMTools.cmake)
  rpmtools_add_rpm_targets(
    "${PROJECT_NAME}"
    "${PROJECT_NAME}.spec.in")
endif()

# -----------------------------------------------------------------------------
# Configuration summary
# -----------------------------------------------------------------------------
message(STATUS "---------------------------------------------")
message(STATUS "CTA configuration summary")
message(STATUS "---------------------------------------------")
message(STATUS "Project")
message(STATUS "CTA_VERSION: ${CTA_VERSION}-${CTA_RELEASE}")
message(STATUS "CTA_LIBVERSION: ${CTA_LIBVERSION} (SOVERSION ${CTA_SOVERSION})")
message(STATUS "SUPPORTED_CATALOGUE_VERSIONS: ${SUPPORTED_CTA_CATALOGUE_SCHEMA_VERSIONS_JOINED}")
message(STATUS "")
message(STATUS "Build")
message(STATUS "PLATFORM: ${PLATFORM} (${CTA_OS_ID} ${CTA_OS_VERSION}, ${CTA_OS_FAMILY}, ${CTA_PACKAGE_FORMAT})")
message(STATUS "CXX_STANDARD: ${PROJECT_CPP_VERSION}")
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
message(STATUS "CMAKE_GENERATOR: ${CMAKE_GENERATOR}")
message(STATUS "JOBS_COUNT: ${JOBS_COUNT}")
message(STATUS "CONFIGURATION_MODE: ${CTA_CONFIGURATION_MODE}")
message(STATUS "ENABLE_ADDRESS_SANITIZER: ${ENABLE_ADDRESS_SANITIZER}")
message(STATUS "ENABLE_CCACHE: ${ENABLE_CCACHE}")
message(STATUS "")
message(STATUS "Features")
message(STATUS "CTA_SCHEDULER_BACKEND: ${CTA_SCHEDULER_BACKEND}")
message(STATUS "CTA_WITH_ORACLE: ${CTA_WITH_ORACLE}")
message(STATUS "CTA_USE_EXTRA_TELEMETRY: ${CTA_USE_EXTRA_TELEMETRY}")
message(STATUS "")
message(STATUS "Testing")
message(STATUS "CTA_ENABLE_TEST_STDOUT_LOGGING: ${CTA_ENABLE_TEST_STDOUT_LOGGING}")
message(STATUS "")
message(STATUS "Packaging")
message(STATUS "CTA_RPM_SOURCE_PACKAGE_ONLY: ${CTA_RPM_SOURCE_PACKAGE_ONLY}")
message(STATUS "CTA_RPM_ENABLE_TARGETS: ${CTA_RPM_ENABLE_TARGETS}")
message(STATUS "CTA_RPM_RUN_UNIT_TESTS: ${CTA_RPM_RUN_UNIT_TESTS}")
message(STATUS "CTA_RPM_BUILD_DEBUG_PACKAGES: ${CTA_RPM_BUILD_DEBUG_PACKAGES}")
message(STATUS "")
message(STATUS "Installation")
message(STATUS "CMAKE_INSTALL_PREFIX: ${CMAKE_INSTALL_PREFIX}")
message(STATUS "CTA_SYSTEMD_UNIT_DIR: ${CTA_SYSTEMD_UNIT_DIR}")
message(STATUS "")
