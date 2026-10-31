# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_library(ctaruntime SHARED
  src/config/ConfigLoader.cpp
  src/config/EnvironmentSubstitution.cpp
  src/config/ValidationResult.cpp
  src/config/parsing/ParseResult.cpp
  src/config/parsing/TomlParser.cpp
  src/health/HealthServer.cpp
  src/signals/SignalReactor.cpp
  src/signals/SignalReactorBuilder.cpp
  src/signals/SignalUtils.cpp)

set_property(TARGET ctaruntime PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctaruntime PROPERTY VERSION "${CTA_LIBVERSION}")
target_link_libraries(ctaruntime PUBLIC ctacommon ctatelemetry)
if(CTA_PACKAGE_FORMAT STREQUAL "deb")
  # Debian ships cpp-httplib as a shared library rather than a header-only library.
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(HTTPLIB REQUIRED IMPORTED_TARGET cpp-httplib)
  target_link_libraries(ctaruntime PRIVATE PkgConfig::HTTPLIB)
endif()
target_include_directories(ctaruntime
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(ctaauth PUBLIC ctaruntime)

install(TARGETS ctaruntime DESTINATION ${CMAKE_INSTALL_LIBDIR})

add_library(ctaruntimeunittests SHARED
  test/ApplicationTest.cpp
  test/cli/ArgParserTest.cpp
  test/config/CommonConfigTest.cpp
  test/config/ConfigLoaderTest.cpp
  test/config/EnvironmentSubstitutionTest.cpp
  test/config/ValidationResultTest.cpp
  test/config/parsing/ReflectionTest.cpp
  test/health/HealthServerTest.cpp
  test/signals/SignalReactorTest.cpp)

set_property(TARGET ctaruntimeunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctaruntimeunittests PROPERTY VERSION "${CTA_LIBVERSION}")
target_link_libraries(ctaruntimeunittests PUBLIC ctaruntime)

if(CTA_BUILD_TEST_PACKAGES)
  install(TARGETS ctaruntimeunittests DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()
