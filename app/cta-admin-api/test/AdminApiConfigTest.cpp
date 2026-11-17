/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "AdminApiConfig.hpp"

#include "common/exception/UserError.hpp"
#include "runtime/config/ConfigLoader.hpp"
#include "tests/TempFile.hpp"

#include <gtest/gtest.h>
#include <string>
#include <toml++/toml.hpp>
#include <utility>
#include <vector>

namespace unitTests {

namespace {

cta::admin_api::AdminApiConfig makeValidConfig() {
  cta::admin_api::AdminApiConfig config;
  config.scheduler.backend_name = "vfsTest";
  config.instance_name = "unitTestInstance";
#ifndef CTA_PGSCHED
  config.scheduler.client.number_of_threads = 500;
#endif
  config.grpc.tls.server_key_path = "/key.pem";
  config.grpc.tls.server_cert_path = "/cert.pem";
  config.auth.kerberos = cta::grpc_server::KerberosAuthConfig {"/file.keytab", "svc/host@REALM"};
  config.auth.enabled_methods = {"kerberos"};
  return config;
}

}  // namespace

TEST(AdminApiConfig, ValidConfigPasses) {
  EXPECT_TRUE(makeValidConfig().validate().ok());
}

TEST(AdminApiConfig, InstanceNameIsRequired) {
  auto config = makeValidConfig();
  config.instance_name = "";
  EXPECT_FALSE(config.validate().ok());
}

TEST(AdminApiConfig, AuthRequiresJwtOrKerberos) {
  auto config = makeValidConfig();
  config.auth.kerberos.reset();
  EXPECT_FALSE(config.validate().ok());
}

TEST(AdminApiConfig, ZeroCatalogueConnectionsIsRejected) {
  auto config = makeValidConfig();
  config.catalogue.client.number_of_connections = 0;
  EXPECT_FALSE(config.validate().ok());
}

TEST(AdminApiConfig, AdminModeDefaultsToAll) {
  EXPECT_EQ(cta::admin_api::AdminApiConfig {}.policies.admin_mode, cta::admin_api::AdminCmdMode::ALL);
}

TEST(AdminApiConfig, AdminModeParsesKnownValues) {
  using cta::admin_api::AdminCmdMode;
  for (const auto& [text, expected] : {
         std::pair {"all",       AdminCmdMode::ALL      },
         std::pair {"repack",    AdminCmdMode::REPACK   },
         std::pair {"no_repack", AdminCmdMode::NO_REPACK}
  }) {
    cta::admin_api::AdminApiConfig config;
    const auto tbl = toml::parse(std::string("[policies]\nadmin_mode = \"") + text + "\"");
    EXPECT_TRUE(cta::runtime::parsing::parseTable(config, tbl, false).ok()) << text;
    EXPECT_EQ(config.policies.admin_mode, expected) << text;
  }
}

TEST(AdminApiConfig, AdminModeRejectsUnknownValue) {
  cta::admin_api::AdminApiConfig config;
  const auto tbl = toml::parse("[policies]\nadmin_mode = \"everything\"");
  EXPECT_FALSE(cta::runtime::parsing::parseTable(config, tbl, false).ok());
}

TEST(AdminApiConfig, AdminModeRejectsNonString) {
  cta::admin_api::AdminApiConfig config;
  const auto tbl = toml::parse("[policies]\nadmin_mode = 1");
  EXPECT_FALSE(cta::runtime::parsing::parseTable(config, tbl, false).ok());
}

TEST(AdminApiConfig, RepackLimitMustBeExplicitButZeroMeansAllFiles) {
  using cta::admin_api::RepackConfig;
  using cta::runtime::loadFromTomlString;
  EXPECT_THROW(loadFromTomlString<RepackConfig>("", "repack.toml", true), cta::exception::UserError);
  EXPECT_EQ(0u, loadFromTomlString<RepackConfig>("max_files_to_select = 0", "repack.toml", true).max_files_to_select);
  EXPECT_EQ(42u, loadFromTomlString<RepackConfig>("max_files_to_select = 42", "repack.toml", true).max_files_to_select);
}

using cta::admin_api::AdminAuthConfig;
using cta::grpc_server::AuthMethod;
using cta::grpc_server::JwtAuthConfig;
using cta::grpc_server::KerberosAuthConfig;

namespace {

JwtAuthConfig makeValidJwtConfig() {
  JwtAuthConfig config;
  config.jwks_uri = "file:///etc/cta/jwks.json";
  config.expected_issuer = "cta";
  config.expected_audience = "cta-admin";
  return config;
}

void checkDefaultJwtAuthentication() {
  AdminAuthConfig config;
  EXPECT_EQ(config.enabled_methods, (std::vector<std::string> {"jwt"}));
  EXPECT_FALSE(config.validate().ok());
  config.jwt = makeValidJwtConfig();
  EXPECT_TRUE(config.validate().ok());

  const std::string document = R"toml(
[jwt]
jwks_uri = "file:///jwks.json"
expected_issuer = "cta"
expected_audience = "api"
min_generation = 0
cache_refresh_interval = 600
pub_key_timeout = 0
jwks_total_timeout = 60
)toml";
  const auto loaded = cta::runtime::loadFromTomlString<AdminAuthConfig>(document, "auth.toml", false);
  EXPECT_TRUE(loaded.methodEnabled(AuthMethod::JWT));
  EXPECT_FALSE(loaded.methodEnabled(AuthMethod::KERBEROS));
  EXPECT_THROW(cta::runtime::loadFromTomlString<AdminAuthConfig>(document, "auth.toml", true),
               cta::exception::UserError);
  EXPECT_NO_THROW(
    cta::runtime::loadFromTomlString<AdminAuthConfig>("enabled_methods = ['jwt']\n" + document, "auth.toml", true));
}

KerberosAuthConfig makeEnabledKerberosConfig() {
  KerberosAuthConfig config;
  config.keytab_path = "/etc/cta/file.keytab";
  config.service_principal = "svc/host@REALM";
  return config;
}

const std::string ADMIN_AUTH_TOML = R"toml(
enabled_methods = ["kerberos"]
[kerberos]
keytab_path = "/etc/cta/file.keytab"
service_principal = "svc/host@REALM"
)toml";

}  // namespace

TEST(AdminAuthConfig, EnumQueriesRespectSupportedMethods) {
  AdminAuthConfig config;
  EXPECT_TRUE(config.methodEnabled(AuthMethod::JWT));
  EXPECT_FALSE(config.methodEnabled(AuthMethod::KERBEROS));

  config.enabled_methods = {"kerberos", "mtls"};
  EXPECT_FALSE(config.methodEnabled(AuthMethod::JWT));
  EXPECT_TRUE(config.methodEnabled(AuthMethod::KERBEROS));
  EXPECT_FALSE(config.methodEnabled(AuthMethod::MTLS));

  config.enabled_methods.clear();
  EXPECT_FALSE(config.methodEnabled(AuthMethod::KERBEROS));
  EXPECT_FALSE(config.methodEnabled(AuthMethod::JWT));
}

TEST(AdminAuthConfig, KerberosOnlyIsValid) {
  AdminAuthConfig config;
  config.enabled_methods.clear();
  config.kerberos = makeEnabledKerberosConfig();
  config.enabled_methods.push_back("kerberos");

  EXPECT_TRUE(config.validate().ok());
  EXPECT_TRUE(config.methodEnabled(AuthMethod::KERBEROS));
  EXPECT_FALSE(config.methodEnabled(AuthMethod::JWT));
}

TEST(AdminAuthConfig, JwtOnlyIsValid) {
  AdminAuthConfig config;
  config.enabled_methods.clear();
  config.jwt = makeValidJwtConfig();
  config.enabled_methods.push_back("jwt");

  EXPECT_TRUE(config.validate().ok());
  EXPECT_TRUE(config.methodEnabled(AuthMethod::JWT));
  EXPECT_FALSE(config.methodEnabled(AuthMethod::KERBEROS));
}

TEST(AdminAuthConfig, JwtAndKerberosIsValid) {
  AdminAuthConfig config;
  config.enabled_methods.clear();
  config.jwt = makeValidJwtConfig();
  config.enabled_methods.push_back("jwt");
  config.kerberos = makeEnabledKerberosConfig();
  config.enabled_methods.push_back("kerberos");

  EXPECT_TRUE(config.validate().ok());
  EXPECT_TRUE(config.methodEnabled(AuthMethod::JWT));
  EXPECT_TRUE(config.methodEnabled(AuthMethod::KERBEROS));
}

TEST(AdminAuthConfig, NoEnabledMethodIsRejected) {
  AdminAuthConfig config;
  config.enabled_methods.clear();
  EXPECT_FALSE(config.validate().ok());

  config.kerberos = makeEnabledKerberosConfig();
  config.enabled_methods.push_back("kerberos");
  config.enabled_methods.clear();
  EXPECT_FALSE(config.validate().ok());
}

TEST(AdminAuthConfig, ValidatesEnabledMethods) {
  AdminAuthConfig config;
  config.enabled_methods.clear();
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

TEST(AdminAuthConfig, DefaultsToJwtButStrictModeRequiresExplicitSelection) {
  checkDefaultJwtAuthentication();
}

TEST(AdminAuthConfig, InactiveJwtSettingsCannotEnableJwtAuthentication) {
  const std::string document = ADMIN_AUTH_TOML + "\n[jwt]\njwks_total_timeout = 'invalid'";
  const auto config = cta::runtime::loadFromTomlString<AdminAuthConfig>(document, "auth.toml", false);
  EXPECT_FALSE(config.jwt);
  EXPECT_FALSE(config.methodEnabled(cta::grpc_server::AuthMethod::JWT));
  EXPECT_TRUE(config.methodEnabled(AuthMethod::KERBEROS));
  EXPECT_FALSE(config.methodEnabled(AuthMethod::JWT));
  EXPECT_THROW(cta::runtime::loadFromTomlString<AdminAuthConfig>(document, "auth.toml", true),
               cta::exception::UserError);
}

}  // namespace unitTests
