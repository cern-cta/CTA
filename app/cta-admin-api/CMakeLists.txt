# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

include_directories(${CMAKE_BINARY_DIR}/lib/protobuf ${PROTOBUF3_INCLUDE_DIRS})

# Admin API implementation: the admin commands (no gRPC), answered with one response (commands/sync/) or streamed
# (commands/stream/), and the gRPC services (rpc/ entry points, rpc/auth/ authentication, rpc/reactors/ streaming
# write reactors)
add_library(cta-admin-api-lib STATIC
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
  ${PROJECT_SOURCE_DIR}/tools/CtaAdminCmdParser.cpp)
target_include_directories(cta-admin-api-lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(cta-admin-api-lib PUBLIC cta-grpc-server-config cta-grpc-server-context cta-grpc-server-core ctagrpc ctaprotobufconversions ctaauth ctacommon ctacatalogue ctascheduler ctaruntime gssapi_krb5)

add_executable(cta-admin-api
  src/AdminApiApp.cpp
  src/cta-admin-api.cpp
)
target_include_directories(cta-admin-api PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(cta-admin-api cta-admin-api-lib ctaruntime ctacommon)

add_library(cta-admin-api-unit-tests SHARED
  test/AdminApiConfigTest.cpp
  test/AdminCmdModeTest.cpp
  test/StreamConvertersTest.cpp
  test/SyncCmdRegistryTest.cpp)
target_include_directories(cta-admin-api-unit-tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(cta-admin-api-unit-tests cta-admin-api-lib cta-grpc-server-config ctacommon ctaruntime gtest)
set_property(TARGET cta-admin-api-unit-tests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET cta-admin-api-unit-tests PROPERTY VERSION "${CTA_LIBVERSION}")

add_manpage (cta-admin-api)

install(TARGETS cta-admin-api DESTINATION ${CMAKE_INSTALL_BINDIR})
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-admin-api.1 DESTINATION ${CMAKE_INSTALL_MANDIR}/man1)
install(FILES cta-admin-api.example.toml ${PROJECT_SOURCE_DIR}/lib/cpp/cta-grpc-server/jwt-revoke-list.example.toml
  DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/doc/cta-admin-api/examples)
