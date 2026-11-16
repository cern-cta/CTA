/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "grpc-server/config/AuthConfig.hpp"

#include "common/exception/UserError.hpp"
#include "grpc-server/config/ListenerConfig.hpp"
#include "runtime/config/ConfigLoader.hpp"
#include "tests/TempFile.hpp"

#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <limits>
#include <string>

namespace unitTests {

using cta::grpc_server::AdminAuthConfig;
using cta::grpc_server::AuthMethod;
using cta::grpc_server::JwtAuthConfig;
using cta::grpc_server::KerberosAuthConfig;
using cta::grpc_server::MtlsAuthConfig;
using cta::grpc_server::WfeAuthConfig;

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

//------------------------------------------------------------------------------
// Mode-specific auth configs
//------------------------------------------------------------------------------

namespace {

KerberosAuthConfig makeEnabledKerberosConfig() {
  KerberosAuthConfig config;
  config.keytab_path = "/etc/cta/file.keytab";
  config.service_principal = "svc/host@REALM";
  return config;
}

MtlsAuthConfig makeEnabledMtlsConfig() {
  MtlsAuthConfig config;
  return config;
}

/// A complete (strict-loadable) AdminAuthConfig document
const std::string ADMIN_AUTH_TOML = R"toml(
enabled_methods = ["kerberos"]
[kerberos]
keytab_path = "/etc/cta/file.keytab"
service_principal = "svc/host@REALM"
)toml";

}  // namespace

TEST(AdminAuthConfig, KerberosOnlyIsValid) {
  AdminAuthConfig config;
  config.kerberos = makeEnabledKerberosConfig();
  config.enabled_methods.push_back("kerberos");

  EXPECT_TRUE(config.validate().ok());
  EXPECT_EQ(config.getEnabledMethods(), (std::set<AuthMethod, std::less<>> {AuthMethod::KERBEROS}));
}

TEST(AdminAuthConfig, JwtOnlyIsValid) {
  AdminAuthConfig config;
  config.jwt = makeValidJwtConfig();
  config.enabled_methods.push_back("jwt");

  EXPECT_TRUE(config.validate().ok());
  EXPECT_EQ(config.getEnabledMethods(), (std::set<AuthMethod, std::less<>> {AuthMethod::JWT}));
}

TEST(AdminAuthConfig, JwtAndKerberosIsValid) {
  AdminAuthConfig config;
  config.jwt = makeValidJwtConfig();
  config.enabled_methods.push_back("jwt");
  config.kerberos = makeEnabledKerberosConfig();
  config.enabled_methods.push_back("kerberos");

  EXPECT_TRUE(config.validate().ok());
  EXPECT_EQ(config.getEnabledMethods().size(), 2U);
}

TEST(AdminAuthConfig, NoEnabledMethodIsRejected) {
  AdminAuthConfig config;
  EXPECT_FALSE(config.validate().ok());

  config.kerberos = makeEnabledKerberosConfig();
  config.enabled_methods.push_back("kerberos");
  config.enabled_methods.clear();
  EXPECT_FALSE(config.validate().ok());
}

TEST(AdminAuthConfig, ValidatesEnabledMethods) {
  AdminAuthConfig config;
  config.kerberos = makeEnabledKerberosConfig();
  config.enabled_methods.push_back("kerberos");
  config.kerberos->keytab_path.clear();

  EXPECT_FALSE(config.validate().ok());
}

TEST(AdminAuthConfig, StrictLoadingAcceptsCompleteDocument) {
  const TempFile file(ADMIN_AUTH_TOML, ".toml");
  EXPECT_NO_THROW(cta::runtime::loadFromToml<AdminAuthConfig>(file.path(), true));
}

TEST(AdminAuthConfig, StrictLoadingRejectsMtlsBlock) {
  const TempFile file(ADMIN_AUTH_TOML + "\n[mtls]\naliases = {}\n", ".toml");
  EXPECT_ANY_THROW(cta::runtime::loadFromToml<AdminAuthConfig>(file.path(), true));
}

TEST(WfeAuthConfig, MtlsOnlyIsValid) {
  WfeAuthConfig config;
  config.mtls = makeEnabledMtlsConfig();
  config.enabled_methods.push_back("mtls");

  EXPECT_TRUE(config.validate().ok());
  EXPECT_EQ(config.getEnabledMethods(), (std::set<AuthMethod, std::less<>> {AuthMethod::MTLS}));
}

TEST(WfeAuthConfig, JwtOnlyIsValid) {
  WfeAuthConfig config;
  config.jwt = makeValidJwtConfig();
  config.enabled_methods.push_back("jwt");

  EXPECT_TRUE(config.validate().ok());
  EXPECT_EQ(config.getEnabledMethods(), (std::set<AuthMethod, std::less<>> {AuthMethod::JWT}));
}

TEST(WfeAuthConfig, NoEnabledMethodIsRejected) {
  WfeAuthConfig config;
  EXPECT_FALSE(config.validate().ok());
}

TEST(WfeAuthConfig, JwtAndMtlsTogetherIsRejected) {
  WfeAuthConfig config;
  config.jwt = makeValidJwtConfig();
  config.enabled_methods.push_back("jwt");
  config.mtls = makeEnabledMtlsConfig();
  config.enabled_methods.push_back("mtls");

  EXPECT_FALSE(config.validate().ok());
}

TEST(WfeAuthConfig, MtlsRejectsEmptyAliasIdentity) {
  WfeAuthConfig config;
  config.mtls = makeEnabledMtlsConfig();
  config.enabled_methods.push_back("mtls");
  config.mtls->aliases["identity"] = {};

  EXPECT_FALSE(config.validate().ok());
}

TEST(WfeAuthConfig, StrictLoadingRejectsKerberosBlock) {
  const TempFile file(R"toml(
enabled_methods = ["mtls"]
[mtls]
aliases = {}
[kerberos]
keytab_path = "/etc/cta/file.keytab"
service_principal = "svc/host@REALM"
)toml",
                      ".toml");
  EXPECT_ANY_THROW(cta::runtime::loadFromToml<WfeAuthConfig>(file.path(), true));
}

}  // namespace unitTests

namespace unitTests {
TEST(ListenerConfig, AutomaticThreadCountIsBoundedAndFixedCountsArePreserved) {
  cta::grpc_server::ListenerConfig config;
  EXPECT_EQ(8, config.resolveThreadCount(0));
  EXPECT_EQ(32, config.resolveThreadCount(4));
  EXPECT_EQ((std::numeric_limits<int>::max() / 8) * 8, config.resolveThreadCount(std::numeric_limits<unsigned>::max()));
  config.number_of_threads = 3;
  EXPECT_EQ(3, config.resolveThreadCount(4));
  config.number_of_threads = -1;
  EXPECT_FALSE(config.validate().ok());
}

TEST(AdminAuthConfig, StrictJwtAllowsAbsentRevocationPathButRequiresTimeouts) {
  const std::string document = R"toml(
enabled_methods = ["jwt"]
[jwt]
jwks_uri = "file:///jwks.json"
expected_issuer = "cta"
expected_audience = "admin"
min_generation = 0
cache_refresh_interval = 600
pub_key_timeout = 0
jwks_total_timeout = 60
)toml";
  const auto config = cta::runtime::loadFromTomlString<AdminAuthConfig>(document, "auth.toml", true);
  EXPECT_FALSE(config.jwt->revoke_list_path);
  auto missingTimeout = document;
  missingTimeout.erase(missingTimeout.find("jwks_total_timeout = 60"));
  EXPECT_THROW(cta::runtime::loadFromTomlString<AdminAuthConfig>(missingTimeout, "auth.toml", true),
               cta::exception::UserError);
  EXPECT_THROW(cta::runtime::loadFromTomlString<AdminAuthConfig>(document + "revoke_list_path = ''", "auth.toml", true),
               cta::exception::UserError);
}
}  // namespace unitTests
