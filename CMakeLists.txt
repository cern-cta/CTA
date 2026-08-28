# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required(VERSION 3.19)

# Package preparation needs metadata, not a compiler toolchain.
project(cta LANGUAGES NONE)

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
set(_cta_pgsched_default OFF)
set(_cta_oracle_default ON)
if(CTA_PACKAGE_FORMAT STREQUAL "deb")
  set(_cta_pgsched_default ON)
  set(_cta_oracle_default OFF)
endif()
option(CTA_USE_PGSCHED "Build with the PostgreSQL scheduler" ${_cta_pgsched_default})
set(CTA_USE_EXTRA_TELEMETRY FALSE CACHE BOOL
  "Build with telemetry instrumentation for postgres performance measurements")
option(CTA_WITH_ORACLE "Build with Oracle catalogue support" ${_cta_oracle_default})
if(CTA_PACKAGE_FORMAT STREQUAL "deb" AND (CTA_WITH_ORACLE OR NOT CTA_USE_PGSCHED))
  message(FATAL_ERROR "Debian builds currently require CTA_USE_PGSCHED=ON and CTA_WITH_ORACLE=OFF")
endif()
include(cmake/Sanitizers.cmake)
option(ENABLE_SCCACHE "Use sccache for compilation" OFF)
option(CTA_ENABLE_TEST_STDOUT_LOGGING "Enable stdout logging in tests" OFF)
option(CTA_SHOW_CONFIGURATION_SUMMARY "Show the configuration summary" ON)
set(CTA_BUILD_MODE "compile" CACHE STRING
  "Build mode: compile, source-package, or binary-package")
set_property(CACHE CTA_BUILD_MODE PROPERTY STRINGS compile source-package binary-package)
option(CTA_RUN_UNIT_TESTS "Run unit tests while building packages" ON)
option(CTA_BUILD_DEBUG_PACKAGES "Build native debug packages" ON)
option(CTA_BUILD_TEST_PACKAGES "Package compiled CTA tests" ON)

set(BUILD_CMDLINE_ONLY FALSE)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "${PROJECT_DEFAULT_BUILD_TYPE}" CACHE STRING "Choose the type of build." FORCE)
else()
  set(CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE}" CACHE STRING "Choose the type of build.")
endif()
set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS Debug Release MinSizeRel RelWithDebInfo)

set(CTA_BUILD_MODES compile source-package binary-package)
if(NOT CTA_BUILD_MODE IN_LIST CTA_BUILD_MODES)
  message(FATAL_ERROR
    "CTA_BUILD_MODE must be one of: compile, source-package, binary-package (got: ${CTA_BUILD_MODE})")
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

if(CTA_BUILD_TEST_PACKAGES)
  set(PACKAGE_BUILD_TEST_PACKAGES 1)
else()
  set(PACKAGE_BUILD_TEST_PACKAGES 0)
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

if(CTA_BUILD_MODE STREQUAL "compile")
  enable_language(C CXX)

  # Applications attach their example configuration checks to this target.
  add_custom_target(cta-example-config-checks)

  set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

  if(ENABLE_SCCACHE)
    find_program(CTA_SCCACHE_EXECUTABLE NAMES sccache REQUIRED)
    set(CMAKE_C_COMPILER_LAUNCHER "${CTA_SCCACHE_EXECUTABLE}")
    set(CMAKE_CXX_COMPILER_LAUNCHER "${CTA_SCCACHE_EXECUTABLE}")
  elseif(CTA_SCCACHE_EXECUTABLE)
    # Clear only CTA's launcher when reconfiguring an existing build without caching.
    foreach(language C CXX)
      if(CMAKE_${language}_COMPILER_LAUNCHER STREQUAL CTA_SCCACHE_EXECUTABLE)
        set(CMAKE_${language}_COMPILER_LAUNCHER "")
      endif()
    endforeach()
  endif()

  add_compile_options(
    "$<$<COMPILE_LANGUAGE:C>:-fPIC;-Wall;-Wextra;-Werror;-Wno-unused-parameter>"
    "$<$<COMPILE_LANGUAGE:CXX>:-fPIC;-pedantic;-Wall;-Wextra;-Werror;-Wno-unused-parameter;-fstack-protector-all;-std=${PROJECT_CPP_VERSION}>"
  )

  cta_enable_sanitizers()

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
endif()

# Share version metadata with packaging; create its library only in compile mode.
include(cmake/CTAVersions.cmake)

# -----------------------------------------------------------------------------
# Full build
# -----------------------------------------------------------------------------
if(CTA_BUILD_MODE STREQUAL "compile")
  set(CMAKE_DISABLE_SOURCE_CHANGES ON)
  list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")
  if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    set(CMAKE_INSTALL_PREFIX "/usr" CACHE PATH "Installation prefix" FORCE)
  endif()
  include(GNUInstallDirs)

  set(CTA_SERVICE_ENV_DIR "${CMAKE_INSTALL_FULL_SYSCONFDIR}/sysconfig")
  if(CTA_PACKAGE_FORMAT STREQUAL "deb")
    set(CTA_SERVICE_ENV_DIR "${CMAKE_INSTALL_FULL_SYSCONFDIR}/default")
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
  find_package(PostgreSQL REQUIRED)
  if(CTA_PACKAGE_FORMAT STREQUAL "deb")
    # Debian's Corrosion 0.5 cannot parse recent rustup toolchain-list output.
    # Let rustup's proxies select the toolchain from rust-toolchain.toml instead.
    set(Rust_RESOLVE_RUSTUP_TOOLCHAINS OFF CACHE BOOL "Resolve Rust tools through rustup")
  endif()
  find_package(Corrosion REQUIRED)

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

  # Apply the test warning policy after all C++ targets have been declared.
  include(cmake/GoogleTestWarnings.cmake)

  # Rust crates
  corrosion_import_crate(MANIFEST_PATH "${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml" LOCKED
    IMPORTED_CRATES CTA_RUST_CRATES)

  # Apply the same cache policy to Rust builds and tests, including local builds.
  set(CTA_RUST_BUILD_ENV "RUSTC_WRAPPER=" "RUSTC_WORKSPACE_WRAPPER=")
  if(ENABLE_SCCACHE)
    set(CTA_RUST_BUILD_ENV
      "RUSTC_WRAPPER=${CTA_SCCACHE_EXECUTABLE}" "RUSTC_WORKSPACE_WRAPPER=" "CARGO_INCREMENTAL=0")
  endif()
  foreach(crate IN LISTS CTA_RUST_CRATES)
    corrosion_set_env_vars(${crate} ${CTA_RUST_BUILD_ENV})
  endforeach()

  # Rust-based tools
  set(CTA_RUST_TOOLS cta-eos-restore-files)

  include(cmake/RustToolArtifacts.cmake)
  foreach(tool IN LISTS CTA_RUST_TOOLS)
    install(PROGRAMS $<TARGET_FILE:${tool}> DESTINATION ${CMAKE_INSTALL_BINDIR})
    cta_add_rust_tool_artifacts(${tool})
  endforeach()

  # Corrosion can't handle tests
  add_custom_target(cta-rust-tests
    COMMAND ${CMAKE_COMMAND} -E env ${CTA_RUST_BUILD_ENV}
      cargo test --locked --workspace
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    COMMENT "Running Rust unit tests with cargo"
    VERBATIM
    DEPENDS ${CTA_RUST_TOOLS}
  )
endif()

# -----------------------------------------------------------------------------
# Packaging
# -----------------------------------------------------------------------------
if(NOT CTA_BUILD_MODE STREQUAL "compile")
  set(CPACK_SOURCE_PACKAGE_FILE_NAME
   "${PROJECT_NAME}-${CTA_VERSION}")
  # Source staging prunes these paths before descending into excluded directories.
  set(CPACK_SOURCE_IGNORE_FILES
    "${CMAKE_SOURCE_DIR}/debian/"
    # Ignore any file or directory starting with a dot
    # This is important for e.g. the caching directories in the gitlab CI
    "/[.]"
    "${CMAKE_BINARY_DIR}/"
    # CI workflows are not needed to build packages. Keep ci/project-json and
    # ci/docker/cta: configuration and cta-release consume files from them.
    # This is something to be refined in the future so that we can just ignore ci/ entirely.
    "${CMAKE_SOURCE_DIR}/ci/build/"
    "${CMAKE_SOURCE_DIR}/ci/checks/"
    "${CMAKE_SOURCE_DIR}/ci/danger/"
    "${CMAKE_SOURCE_DIR}/ci/docker/pipeline/"
    "${CMAKE_SOURCE_DIR}/ci/docs/"
    "${CMAKE_SOURCE_DIR}/ci/orchestration/"
    "${CMAKE_SOURCE_DIR}/ci/release/"
    "${CMAKE_SOURCE_DIR}/ci/sbom/"
    "${CMAKE_SOURCE_DIR}/ci/system_tests/"
    "${CMAKE_SOURCE_DIR}/ci/utils/"
    "/build/"
    "/pod_logs/"
    "/test_logs/"
    "${CMAKE_SOURCE_DIR}/tmp/"
    "[.]core$"
    # Python caches and non-hidden virtual environments
    "/__pycache__/"
    "[.]py[co]$"
    "/(venv|env)/"
    # Local logs, temporary files, backups, and editor swap files
    "[.](log|tmp|bak|swp|swo|swn|swm|swd)$"
    # The documentation site is not needed to build packages.
    "${CMAKE_SOURCE_DIR}/docs/"
    # Keep excluding directories from previous versions in case they were not cleaned up
    "/build_srpm/"
    "/build_rpm/"
    "/image_rpms/"
    "/assets/"
    "/([^/]+/)*target/")
  # Use CMake's recursive copy instead of CPack's whole-tree enumeration.
  set(CPACK_SOURCE_INSTALLED_DIRECTORIES "")
  configure_file(cmake/StageSourcePackage.cmake.in StageSourcePackage.cmake @ONLY)
  include(CPack)
  # Register the staging hook only for source archives, not binary CPack installs.
  file(APPEND "${CPACK_SOURCE_OUTPUT_CONFIG_FILE}"
    "\nset(CPACK_INSTALL_SCRIPTS \"${CMAKE_CURRENT_BINARY_DIR}/StageSourcePackage.cmake\")\n")

  if(CTA_PACKAGE_FORMAT STREQUAL "rpm")
    include(cmake/RPMTools.cmake)
    rpmtools_add_rpm_targets(
      "${PROJECT_NAME}"
      "packaging/rpm/${PROJECT_NAME}.spec.in"
      "${CTA_BUILD_MODE}"
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
  message(STATUS "CTA_VERSION: ${CTA_VERSION}")
  message(STATUS "CTA_LIBVERSION: ${CTA_LIBVERSION} (SOVERSION ${CTA_SOVERSION})")
  message(STATUS "SUPPORTED_CATALOGUE_VERSIONS: ${SUPPORTED_CTA_CATALOGUE_SCHEMA_VERSIONS_JOINED}")
  message(STATUS "")
  message(STATUS "Build")
  message(STATUS "PLATFORM: ${PLATFORM} (${CTA_OS_ID} ${CTA_OS_VERSION}, ${CTA_OS_FAMILY}, ${CTA_PACKAGE_FORMAT})")
  message(STATUS "CMAKE_VERSION: ${CMAKE_VERSION}")
  message(STATUS "CMAKE_SYSTEM_PROCESSOR: ${CMAKE_SYSTEM_PROCESSOR}")
  if(CTA_BUILD_MODE STREQUAL "compile")
    message(STATUS "CXX_COMPILER: ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
  endif()
  message(STATUS "CXX_STANDARD: ${PROJECT_CPP_VERSION}")
  message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
  message(STATUS "CMAKE_GENERATOR: ${CMAKE_GENERATOR}")
  message(STATUS "JOBS_COUNT: ${JOBS_COUNT}")
  message(STATUS "CTA_BUILD_MODE: ${CTA_BUILD_MODE}")
  message(STATUS "CTA_SANITIZERS: ${CTA_SANITIZERS}")
  message(STATUS "ENABLE_SCCACHE: ${ENABLE_SCCACHE}")
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
  message(STATUS "CTA_RUN_UNIT_TESTS: ${CTA_RUN_UNIT_TESTS}")
  message(STATUS "CTA_BUILD_DEBUG_PACKAGES: ${CTA_BUILD_DEBUG_PACKAGES}")
  message(STATUS "CTA_BUILD_TEST_PACKAGES: ${CTA_BUILD_TEST_PACKAGES}")
  message(STATUS "")
  message(STATUS "Installation")
  message(STATUS "CMAKE_INSTALL_PREFIX: ${CMAKE_INSTALL_PREFIX}")
  message(STATUS "CTA_SYSTEMD_UNIT_DIR: ${CTA_SYSTEMD_UNIT_DIR}")
  message(STATUS "")
endif()
