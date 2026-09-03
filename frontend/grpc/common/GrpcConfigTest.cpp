/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "frontend/grpc/common/GrpcConfig.hpp"

#include <gtest/gtest.h>

namespace unitTests {

using cta::frontend::grpc::common::JwtAuthConfig;

namespace {

JwtAuthConfig makeValidJwtConfig() {
  JwtAuthConfig config;
  config.enabled = true;
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

TEST(JwtAuthConfig, PubKeyTimeoutIsNotValidatedWhenDisabled) {
  auto config = makeValidJwtConfig();
  config.enabled = false;
  config.cache_refresh_interval = 600;
  config.pub_key_timeout = 1;

  EXPECT_TRUE(config.validate().ok());
}

}  // namespace unitTests
