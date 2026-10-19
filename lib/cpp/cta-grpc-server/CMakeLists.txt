# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# gRPC server configuration schemas; kept free of gRPC so that clients and tests can use it cheaply
add_library(cta-grpc-server-config STATIC
  src/config/AuthConfig.cpp
  src/config/ListenerConfig.cpp
  src/config/TlsConfig.cpp)

target_include_directories(cta-grpc-server-config PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(cta-grpc-server-config PUBLIC ctaruntime ctacommon)

# Plumbing shared by cta-admin-api and cta-workflow-api: JWT/TLS authentication, request tracking
# and the gRPC server builder with the settings common to the CTA gRPC servers
add_library(cta-grpc-server-core STATIC
  src/auth/Authentication.cpp
  src/auth/JwtAuth.cpp
  src/server/RequestTracker.cpp
  src/server/ServerBuilderHelper.cpp
  src/server/TlsCredentials.cpp)

target_include_directories(cta-grpc-server-core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(cta-grpc-server-core PUBLIC cta-grpc-server-config ctaauth ctacommon ctatelemetryfrontend gRPC::grpc++ gRPC::grpc++_reflection)

# Objects needed by the RPC services (Catalogue, Scheduler DB, Scheduler) of the admin and workflow gRPC servers
add_library(cta-grpc-server-context STATIC src/server/ServiceContext.cpp)
target_include_directories(cta-grpc-server-context PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
if(CTA_USE_PGSCHED)
  target_link_libraries(cta-grpc-server-context PUBLIC cta-grpc-server-config ctacommon ctacatalogue ctascheduler ctaruntime)
else()
  target_link_libraries(cta-grpc-server-context PUBLIC cta-grpc-server-config ctacommon ctaobjectstore ctacatalogue ctascheduler ctaruntime)
endif()

# Unit tests for the shared gRPC server library
add_library(cta-grpc-server-unit-tests SHARED
  test/AuthConfigTest.cpp
  test/ServiceContextTest.cpp)
target_link_libraries(cta-grpc-server-unit-tests cta-grpc-server-config cta-grpc-server-context ctacommon ctaruntime gtest)
set_property(TARGET cta-grpc-server-unit-tests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET cta-grpc-server-unit-tests PROPERTY VERSION "${CTA_LIBVERSION}")
