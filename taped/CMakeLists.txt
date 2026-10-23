# SPDX-FileCopyrightText: 2021 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_subdirectory (scsi)
add_subdirectory (drive)
add_subdirectory (system)
add_subdirectory (file)
add_subdirectory (session)
add_subdirectory (rao)
add_subdirectory (daemon)

# EL9's packaged GoogleTest 1.11 leaves matcher buffer storage uninitialized, triggering GCC 14 warnings.
# Suppress them only in affected mock users; other warnings remain errors.
# Remove when EL9 provides a fixed GoogleTest package and these files build without the suppression (or when we remove EL9).
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 14)
  foreach(mock_source IN ITEMS
      scsi/DeviceTest.cpp
      drive/DriveTest.cpp
      system/Wrapper.cpp
      session/TaskWatchDogTest.cpp
      daemon/DriveHandlerTests.cpp)
    get_filename_component(mock_directory "${mock_source}" DIRECTORY)
    set_property(SOURCE "${CMAKE_CURRENT_SOURCE_DIR}/${mock_source}"
      DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/${mock_directory}"
      APPEND PROPERTY COMPILE_OPTIONS -Wno-uninitialized -Wno-maybe-uninitialized)
  endforeach()
endif()

add_executable (cta-taped cta-taped.cpp)
target_link_libraries(cta-taped ctatapedaemon ctacommon ctatelemetry)

add_manpage(cta-taped)

install (TARGETS cta-taped DESTINATION ${CMAKE_INSTALL_BINDIR})
install (FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-taped.1 DESTINATION ${CMAKE_INSTALL_MANDIR}/man1)
install (FILES cta-taped.sysconfig DESTINATION ${CTA_SERVICE_ENV_DIR} RENAME cta-taped)

# CTA's cta-taped integration tests.
add_library(cta-taped-integration-tests SHARED
  cta-tapedIntegrationTests.cpp)
set_property(TARGET cta-taped-integration-tests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET cta-taped-integration-tests PROPERTY VERSION "${CTA_LIBVERSION}")

target_link_libraries(cta-taped-integration-tests
  cta-unit-test-helper
  ctacommon)

if(CTA_BUILD_TEST_PACKAGES)
  install(TARGETS cta-taped-integration-tests DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()
