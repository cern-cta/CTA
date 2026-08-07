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

add_library(ctatapedutils
  TapedUtils.cpp
)
target_link_libraries(ctatapedutils PUBLIC ctaruntime)
target_link_libraries(ctatapedaemon ctatapedutils)

add_library(ctataped
  TapedApp.cpp
)

target_link_libraries(ctataped
  ctacommon
  ctaruntime
  ctatelemetrytaped
  ctatapedaemon
  ctatapedutils
)

add_executable (cta-taped cta-taped.cpp)
target_link_libraries(cta-taped ctataped ctaruntime)

if(NOT CTA_USE_PGSCHED)
  set(CTA_TAPED_EXAMPLE_CONFIG ${CMAKE_CURRENT_SOURCE_DIR}/cta-taped.example.toml)
else()
  set(CTA_TAPED_EXAMPLE_CONFIG ${CMAKE_CURRENT_SOURCE_DIR}/cta-taped.pg.example.toml)
endif()

add_custom_target(cta-taped-config-check
  COMMAND $<TARGET_FILE:cta-taped>
    --config-strict
    --config ${CTA_TAPED_EXAMPLE_CONFIG}
    --config-check
  DEPENDS cta-taped
  COMMENT "Validating the cta-taped example configuration" VERBATIM)
add_dependencies(cta-example-config-checks cta-taped-config-check)

add_manpage (cta-taped)

install (TARGETS cta-taped DESTINATION ${CMAKE_INSTALL_BINDIR})
install (FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-taped.1 DESTINATION ${CMAKE_INSTALL_MANDIR}/man1)
if(NOT CTA_USE_PGSCHED)
  install (FILES cta-taped.example.toml
    DESTINATION ${CMAKE_INSTALL_DOCDIR}
    PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)
else()
  install (FILES cta-taped.pg.example.toml
    DESTINATION ${CMAKE_INSTALL_DOCDIR}
    RENAME cta-taped.example.toml
    PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)
endif()
