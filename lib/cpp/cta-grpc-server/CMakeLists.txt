# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# gRPC server configuration schemas; kept free of gRPC so that clients and tests can use it cheaply
add_library(cta-grpc-server-config STATIC
  src/config/AuthConfig.cpp
  src/config/ListenerConfig.cpp
  src/config/TlsConfig.cpp)

target_include_directories(cta-grpc-server-config PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(cta-grpc-server-config PUBLIC ctaruntime ctacommon)

# Authentication of gRPC requests: JWT validation (JWKS cache, revoke list) and mTLS identities
find_package(OpenSSL REQUIRED)
add_library(cta-grpc-server-auth STATIC
  src/auth/Authentication.cpp
  src/auth/jwt/JwkCache.cpp
  src/auth/jwt/JwksFetcher.cpp
  src/auth/jwt/JwtAuthFactory.cpp
  src/auth/jwt/JwtAuthManager.cpp)

target_include_directories(cta-grpc-server-auth PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include ${CMAKE_SOURCE_DIR}/common/jwt-cpp/include)
target_link_libraries(cta-grpc-server-auth PUBLIC cta-grpc-server-config ctaruntime ctacommon gRPC::grpc++ PRIVATE curl OpenSSL::Crypto)

# Plumbing shared by cta-admin-api and cta-workflow-api: TLS credentials, request logging and tracking
# and the gRPC server builder with the settings common to the CTA gRPC servers
add_library(cta-grpc-server STATIC
  src/server/RequestLogger.cpp
  src/server/RequestTracker.cpp
  src/server/ServerBuilderHelper.cpp
  src/server/TlsCredentials.cpp)

target_include_directories(cta-grpc-server PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(cta-grpc-server PUBLIC cta-grpc-server-config ctacommon ctatelemetryfrontend gRPC::grpc++ gRPC::grpc++_reflection)

# Unit tests for the shared gRPC server library
add_library(cta-grpc-server-unit-tests SHARED
  test/AuthConfigTest.cpp
  test/auth/jwt/JwkCacheTest.cpp
  test/auth/jwt/JwtValidationTest.cpp
  test/server/RequestLoggerTest.cpp)
target_link_libraries(cta-grpc-server-unit-tests cta-grpc-server-config cta-grpc-server-auth cta-grpc-server ctacommon ctaruntime gtest)
set_property(TARGET cta-grpc-server-unit-tests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET cta-grpc-server-unit-tests PROPERTY VERSION "${CTA_LIBVERSION}")

if(CTA_BUILD_TEST_PACKAGES)
  install(TARGETS cta-grpc-server-unit-tests DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()
