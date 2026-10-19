# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_library(cta-workflow-api-lib STATIC
  src/CtaRpcWfeImpl.cpp
  src/WorkflowEvent.cpp
)
target_include_directories(cta-workflow-api-lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(cta-workflow-api-lib PUBLIC cta-grpc-server-config cta-grpc-server cta-service-resources ctagrpc ctaprotobufconversions cta-grpc-server-auth ctacommon ctacatalogue ctascheduler ctaruntime ctatelemetryfrontend)

add_executable(cta-workflow-api
  src/WorkflowApiApp.cpp
  src/cta-workflow-api.cpp
)
target_include_directories(cta-workflow-api PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(cta-workflow-api cta-workflow-api-lib ctaruntime ctacommon)

add_library(cta-workflow-api-unit-tests SHARED
  test/WorkflowApiConfigTest.cpp
  test/WorkflowEventTest.cpp)
target_include_directories(cta-workflow-api-unit-tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(cta-workflow-api-unit-tests cta-workflow-api-lib cta-grpc-server-config ctacommon ctaruntime gtest)
set_property(TARGET cta-workflow-api-unit-tests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET cta-workflow-api-unit-tests PROPERTY VERSION "${CTA_LIBVERSION}")

add_manpage (cta-workflow-api)

install(TARGETS cta-workflow-api DESTINATION ${CMAKE_INSTALL_BINDIR})
if(CTA_BUILD_TEST_PACKAGES)
  install(TARGETS cta-workflow-api-unit-tests DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-workflow-api.1 DESTINATION ${CMAKE_INSTALL_MANDIR}/man1)
install(FILES cta-workflow-api.example.toml ${PROJECT_SOURCE_DIR}/lib/cpp/cta-grpc-server/jwt-revoke-list.example.toml
  DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/doc/cta-workflow-api/examples)
