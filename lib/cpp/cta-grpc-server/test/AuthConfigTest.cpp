/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "grpc-server/config/AuthConfig.hpp"

#include "grpc-server/config/ListenerConfig.hpp"
#include "grpc-server/server/ServerBuilderHelper.hpp"

#include <gtest/gtest.h>
#include <limits>

namespace unitTests {

using cta::grpc_server::JwtAuthConfig;

namespace {

JwtAuthConfig makeValidJwtConfig() {
  JwtAuthConfig config;
  config.jwks_uri = "file:///etc/cta/jwks.json";
  config.expected_issuer = "cta";
  config.expected_audience = "cta-admin";
  return config;
}

}  // namespace

TEST(JwtAuthConfig, DefaultPubKeyTimeoutIsValid) {
  auto config = makeValidJwtConfig();
  ASSERT_EQ(config.pub_key_timeout, 0);

  EXPECT_TRUE(config.validate().ok());
}

TEST(JwtAuthConfig, ZeroPubKeyTimeoutIsValidBelowCacheRefreshInterval) {
  auto config = makeValidJwtConfig();
  config.cache_refresh_interval = 600;
  config.pub_key_timeout = 0;

  EXPECT_TRUE(config.validate().ok());
}

TEST(JwtAuthConfig, NonZeroPubKeyTimeoutBelowCacheRefreshIntervalIsRejected) {
  auto config = makeValidJwtConfig();
  config.cache_refresh_interval = 600;
  config.pub_key_timeout = 599;

  const auto result = config.validate();
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.what(),
            "1) Field 'pub_key_timeout' must be zero or greater than or equal to 'cache_refresh_interval'.\n");
}

TEST(JwtAuthConfig, PubKeyTimeoutEqualToCacheRefreshIntervalIsValid) {
  auto config = makeValidJwtConfig();
  config.cache_refresh_interval = 600;
  config.pub_key_timeout = 600;

  EXPECT_TRUE(config.validate().ok());
}

TEST(JwtAuthConfig, PubKeyTimeoutAboveCacheRefreshIntervalIsValid) {
  auto config = makeValidJwtConfig();
  config.cache_refresh_interval = 600;
  config.pub_key_timeout = 601;

  EXPECT_TRUE(config.validate().ok());
}

TEST(JwtAuthConfig, PubKeyTimeoutIsValidatedWheneverSectionIsPresent) {
  auto config = makeValidJwtConfig();
  config.cache_refresh_interval = 600;
  config.pub_key_timeout = 1;

  EXPECT_FALSE(config.validate().ok());
}

TEST(ServerBuilderHelper, AutomaticThreadLimitHandlesUnknownCpuCountAndOverflow) {
  EXPECT_EQ(8, cta::grpc_server::automaticThreadLimit(0));
  EXPECT_EQ(32, cta::grpc_server::automaticThreadLimit(4));
  EXPECT_EQ((std::numeric_limits<int>::max() / 8) * 8,
            cta::grpc_server::automaticThreadLimit(std::numeric_limits<unsigned>::max()));
}

TEST(ListenerConfig, RejectsNegativeThreadCount) {
  cta::grpc_server::ListenerConfig config;
  config.number_of_threads = -1;
  EXPECT_FALSE(config.validate().ok());
}

}  // namespace unitTests
