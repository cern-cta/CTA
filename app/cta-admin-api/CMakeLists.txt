# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

include_directories(${CMAKE_BINARY_DIR}/lib/protobuf ${PROTOBUF3_INCLUDE_DIRS})

# Library
add_library(cta-admin-api-lib STATIC
  src/commands/AdminCmdLogParams.cpp
  src/commands/AdminCmdOptions.cpp
  src/commands/stream/StreamFactory.cpp
  src/commands/stream/converters/Disk.cpp
  src/commands/stream/converters/Libraries.cpp
  src/commands/stream/converters/MountRules.cpp
  src/commands/stream/converters/Organization.cpp
  src/commands/stream/converters/Storage.cpp
  src/commands/stream/converters/Version.cpp
  src/commands/stream/scheduling/DriveLsResponseStream.cpp
  src/commands/stream/scheduling/FailedRequestLsResponseStream.cpp
  src/commands/stream/scheduling/RepackLsResponseStream.cpp
  src/commands/stream/scheduling/ShowQueuesResponseStream.cpp
  src/commands/stream/tapes/RecycleTapeFileLsResponseStream.cpp
  src/commands/stream/tapes/TapeFileLsResponseStream.cpp
  src/commands/stream/tapes/TapeLsResponseStream.cpp
  src/commands/sync/SyncCmd.cpp
  src/commands/sync/SyncCmdRegistry.cpp
  src/commands/sync/handlers/Disk.cpp
  src/commands/sync/handlers/Libraries.cpp
  src/commands/sync/handlers/MountRules.cpp
  src/commands/sync/handlers/Organization.cpp
  src/commands/sync/handlers/Scheduling.cpp
  src/commands/sync/handlers/Storage.cpp
  src/commands/sync/handlers/Tapes.cpp
  src/rpc/CtaRpcStreamImpl.cpp
  src/rpc/CtaRpcSyncImpl.cpp
  src/rpc/auth/RequestAuthenticator.cpp
  src/rpc/auth/kerberos/NegotiationService.cpp
  src/rpc/auth/kerberos/ServerNegotiationRequestHandler.cpp
  src/rpc/auth/kerberos/TokenStore.cpp
  src/rpc/reactors/CtaAdminServerWriteReactor.cpp
  src/AdminApiApp.cpp
  ${PROJECT_SOURCE_DIR}/tools/CtaAdminCmdParser.cpp
)
target_include_directories(cta-admin-api-lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(cta-admin-api-lib PUBLIC
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
  gssapi_krb5
)

# Unit tests
add_library(cta-admin-api-unit-tests SHARED
  test/AdminApiConfigTest.cpp
  test/AdminCmdModeTest.cpp
  test/StreamConvertersTest.cpp
  test/SyncCmdRegistryTest.cpp)
target_include_directories(cta-admin-api-unit-tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(cta-admin-api-unit-tests cta-admin-api-lib cta-grpc-server-config ctacommon ctaruntime gtest)
set_property(TARGET cta-admin-api-unit-tests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET cta-admin-api-unit-tests PROPERTY VERSION "${CTA_LIBVERSION}")

# Executable
add_executable(cta-admin-api
  src/cta-admin-api.cpp
)
target_include_directories(cta-admin-api PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(cta-admin-api
  cta-admin-api-lib
  ctaruntime
  ctacommon
)

if(CTA_USE_PGSCHED)
  set(CTA_ADMIN_API_EXAMPLE_CONFIG ${CMAKE_CURRENT_SOURCE_DIR}/cta-admin-api.pg.example.toml)
else()
  set(CTA_ADMIN_API_EXAMPLE_CONFIG ${CMAKE_CURRENT_SOURCE_DIR}/cta-admin-api.example.toml)
endif()

# Config checker
add_custom_target(cta-admin-api-config-check
  COMMAND $<TARGET_FILE:cta-admin-api>
    # TODO: switch to strict once cta/CTA#1743 is closed
    # --config-strict
    --config ${CTA_ADMIN_API_EXAMPLE_CONFIG}
    --config-check
  DEPENDS cta-admin-api
  COMMENT "Validating the cta-admin-api example configuration" VERBATIM)
add_dependencies(cta-example-config-checks cta-admin-api-config-check)

# Man pages
add_manpage (cta-admin-api)

# Install
install(TARGETS cta-admin-api DESTINATION ${CMAKE_INSTALL_BINDIR})
if(CTA_BUILD_TEST_PACKAGES)
  install(TARGETS cta-admin-api-unit-tests DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-admin-api.1 DESTINATION ${CMAKE_INSTALL_MANDIR}/man1)
install(FILES ${CTA_ADMIN_API_EXAMPLE_CONFIG}
  DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/doc/cta-admin-api/examples
  RENAME cta-admin-api.example.toml
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
)
install(FILES ${PROJECT_SOURCE_DIR}/lib/cpp/cta-grpc-server/jwt-revoke-list.example.toml
  DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/doc/cta-admin-api/examples)
