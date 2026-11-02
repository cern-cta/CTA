# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Library
add_library(cta-workflow-api-lib STATIC
  src/CtaRpcWfeImpl.cpp
  src/WorkflowEvent.cpp
  src/WorkflowApiApp.cpp
)
target_include_directories(cta-workflow-api-lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(cta-workflow-api-lib PUBLIC
  cta-grpc-server-auth
  cta-grpc-server-config
  cta-grpc-server
  cta-service-resources
  ctagrpc
  ctaprotobufconversions
  ctacommon
  ctacatalogue
  ctascheduler
  ctaruntime
  ctatelemetryfrontend
)

# Unit tests
add_library(cta-workflow-api-unit-tests SHARED
  test/WorkflowApiConfigTest.cpp
  test/WorkflowEventTest.cpp)
target_include_directories(cta-workflow-api-unit-tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(cta-workflow-api-unit-tests cta-workflow-api-lib cta-grpc-server-config ctacommon ctaruntime gtest)
set_property(TARGET cta-workflow-api-unit-tests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET cta-workflow-api-unit-tests PROPERTY VERSION "${CTA_LIBVERSION}")

# Executable
add_executable(cta-workflow-api
  src/cta-workflow-api.cpp
)
target_include_directories(cta-workflow-api PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(cta-workflow-api
  cta-workflow-api-lib
  ctaruntime
  ctacommon
)

if(CTA_USE_PGSCHED)
  set(CTA_WF_API_EXAMPLE_CONFIG ${CMAKE_CURRENT_SOURCE_DIR}/cta-workflow-api.pg.example.toml)
else()
  set(CTA_WF_API_EXAMPLE_CONFIG ${CMAKE_CURRENT_SOURCE_DIR}/cta-workflow-api.example.toml)
endif()

# Config checker
add_custom_target(cta-workflow-api-config-check
  COMMAND $<TARGET_FILE:cta-workflow-api>
    # TODO: switch to strict once cta/CTA#1743 is closed
    # --config-strict
    --config ${CTA_WF_API_EXAMPLE_CONFIG}
    --config-check
  DEPENDS cta-workflow-api
  COMMENT "Validating the cta-workflow-api example configuration" VERBATIM)
add_dependencies(cta-example-config-checks cta-workflow-api-config-check)

# Man pages
add_manpage (cta-workflow-api)

# Install
install(TARGETS cta-workflow-api DESTINATION ${CMAKE_INSTALL_BINDIR})
if(CTA_BUILD_TEST_PACKAGES)
  install(TARGETS cta-workflow-api-unit-tests DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-workflow-api.1 DESTINATION ${CMAKE_INSTALL_MANDIR}/man1)
install(FILES ${CTA_WF_API_EXAMPLE_CONFIG}
  DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/doc/cta-workflow-api/examples
  RENAME cta-workflow-api.example.toml
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
)
install(FILES ${PROJECT_SOURCE_DIR}/lib/cpp/cta-grpc-server/jwt-revoke-list.example.toml
  DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/doc/cta-workflow-api/examples)
