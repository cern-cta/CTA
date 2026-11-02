/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "grpc-server/config/AuthConfig.hpp"

#include "common/exception/UserError.hpp"
#include "runtime/config/ConfigLoader.hpp"

#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
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

//------------------------------------------------------------------------------
// Mode-specific auth configs
//------------------------------------------------------------------------------

namespace {

KerberosAuthConfig makeEnabledKerberosConfig() {
  KerberosAuthConfig config;
  config.enabled = true;
  config.keytab_path = "/etc/cta/file.keytab";
  config.service_principal = "svc/host@REALM";
  return config;
}

MtlsAuthConfig makeEnabledMtlsConfig() {
  MtlsAuthConfig config;
  config.enabled = true;
  return config;
}

/// Temporary TOML file, removed on destruction
class TomlFile {
public:
  explicit TomlFile(const std::string& content)
      : m_path(std::filesystem::temp_directory_path() / ("AuthConfigTest-" + std::to_string(s_counter++) + ".toml")) {
    std::ofstream(m_path) << content;
  }

  ~TomlFile() { std::filesystem::remove(m_path); }

  std::string path() const { return m_path.string(); }

private:
  std::filesystem::path m_path;
  static inline int s_counter = 0;
};

/// A complete (strict-loadable) AdminAuthConfig document
const std::string ADMIN_AUTH_TOML = R"toml(
[jwt]
enabled = false
jwks_uri = ""
cache_refresh_interval = 600
pub_key_timeout = 0
jwks_total_timeout = 60
expected_issuer = ""
expected_audience = ""
min_generation = 0
revoke_list_path = ""

[kerberos]
enabled = true
keytab_path = "/etc/cta/file.keytab"
service_principal = "svc/host@REALM"
)toml";

}  // namespace

TEST(AdminAuthConfig, KerberosOnlyIsValid) {
  AdminAuthConfig config;
  config.kerberos = makeEnabledKerberosConfig();

  EXPECT_TRUE(config.validate().ok());
  EXPECT_EQ(config.getEnabledMethods(), (std::set<AuthMethod, std::less<>> {AuthMethod::KERBEROS}));
}

TEST(AdminAuthConfig, JwtOnlyIsValid) {
  AdminAuthConfig config;
  config.jwt = makeValidJwtConfig();

  EXPECT_TRUE(config.validate().ok());
  EXPECT_EQ(config.getEnabledMethods(), (std::set<AuthMethod, std::less<>> {AuthMethod::JWT}));
}

TEST(AdminAuthConfig, JwtAndKerberosIsValid) {
  AdminAuthConfig config;
  config.jwt = makeValidJwtConfig();
  config.kerberos = makeEnabledKerberosConfig();

  EXPECT_TRUE(config.validate().ok());
  EXPECT_EQ(config.getEnabledMethods().size(), 2U);
}

TEST(AdminAuthConfig, NoEnabledMethodThrows) {
  AdminAuthConfig config;
  EXPECT_THROW(config.validate(), cta::exception::UserError);

  config.kerberos = makeEnabledKerberosConfig();
  config.kerberos->enabled = false;
  EXPECT_THROW(config.validate(), cta::exception::UserError);
}

TEST(AdminAuthConfig, ValidatesEnabledMethods) {
  AdminAuthConfig config;
  config.kerberos = makeEnabledKerberosConfig();
  config.kerberos->keytab_path.clear();

  EXPECT_FALSE(config.validate().ok());
}

TEST(AdminAuthConfig, StrictLoadingAcceptsCompleteDocument) {
  const TomlFile file(ADMIN_AUTH_TOML);
  EXPECT_NO_THROW(cta::runtime::loadFromToml<AdminAuthConfig>(file.path(), true));
}

TEST(AdminAuthConfig, StrictLoadingRejectsMtlsBlock) {
  const TomlFile file(ADMIN_AUTH_TOML + "\n[mtls]\nenabled = true\n");
  EXPECT_ANY_THROW(cta::runtime::loadFromToml<AdminAuthConfig>(file.path(), true));
}

TEST(WfeAuthConfig, MtlsOnlyIsValid) {
  WfeAuthConfig config;
  config.mtls = makeEnabledMtlsConfig();

  EXPECT_TRUE(config.validate().ok());
  EXPECT_EQ(config.getEnabledMethods(), (std::set<AuthMethod, std::less<>> {AuthMethod::MTLS}));
}

TEST(WfeAuthConfig, JwtOnlyIsValid) {
  WfeAuthConfig config;
  config.jwt = makeValidJwtConfig();

  EXPECT_TRUE(config.validate().ok());
  EXPECT_EQ(config.getEnabledMethods(), (std::set<AuthMethod, std::less<>> {AuthMethod::JWT}));
}

TEST(WfeAuthConfig, NoEnabledMethodThrows) {
  WfeAuthConfig config;
  EXPECT_THROW(config.validate(), cta::exception::UserError);
}

TEST(WfeAuthConfig, JwtAndMtlsTogetherThrows) {
  WfeAuthConfig config;
  config.jwt = makeValidJwtConfig();
  config.mtls = makeEnabledMtlsConfig();

  EXPECT_THROW(config.validate(), cta::exception::UserError);
}

TEST(WfeAuthConfig, MtlsRejectsEmptyAliasIdentity) {
  WfeAuthConfig config;
  config.mtls = makeEnabledMtlsConfig();
  config.mtls->aliases["identity"] = {};

  EXPECT_FALSE(config.validate().ok());
}

TEST(WfeAuthConfig, StrictLoadingRejectsKerberosBlock) {
  const TomlFile file(R"toml(
[jwt]
enabled = false
jwks_uri = ""
cache_refresh_interval = 600
pub_key_timeout = 0
jwks_total_timeout = 60
expected_issuer = ""
expected_audience = ""
min_generation = 0
revoke_list_path = ""

[mtls]
enabled = true

[mtls.aliases]

[kerberos]
enabled = true
keytab_path = "/etc/cta/file.keytab"
service_principal = "svc/host@REALM"
)toml");
  EXPECT_ANY_THROW(cta::runtime::loadFromToml<WfeAuthConfig>(file.path(), true));
}

}  // namespace unitTests
