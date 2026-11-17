/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "WorkflowApiConfig.hpp"

#include "common/exception/UserError.hpp"
#include "runtime/config/ConfigLoader.hpp"
#include "tests/TempFile.hpp"

#include <gtest/gtest.h>
#include <string>
#include <vector>

namespace unitTests {

namespace {

cta::workflow_api::WorkflowApiConfig makeValidConfig() {
  cta::workflow_api::WorkflowApiConfig config;
  config.scheduler.backend_name = "vfsTest";
  config.instance_name = "unitTestInstance";
#ifndef CTA_PGSCHED
  config.scheduler.client.number_of_threads = 500;
#endif
  config.grpc.tls.server_key_path = "/key.pem";
  config.grpc.tls.server_cert_path = "/cert.pem";
  config.auth.mtls = cta::grpc_server::MtlsAuthConfig {{{"instance", {"host.example.org"}}}};
  config.auth.enabled_methods = {"mtls"};
  return config;
}

}  // namespace

TEST(WorkflowApiConfig, ValidConfigPasses) {
  EXPECT_TRUE(makeValidConfig().validate().ok());
}

TEST(WorkflowApiConfig, InstanceNameIsRequired) {
  auto config = makeValidConfig();
  config.instance_name = "";
  EXPECT_FALSE(config.validate().ok());
}

TEST(WorkflowApiConfig, AuthRequiresJwtOrMtls) {
  auto config = makeValidConfig();
  config.auth.mtls.reset();
  EXPECT_FALSE(config.validate().ok());
}

TEST(WorkflowApiConfig, JwtAndMtlsTogetherAreRejected) {
  auto config = makeValidConfig();
  config.auth.jwt = cta::grpc_server::JwtAuthConfig {};
  config.auth.enabled_methods.push_back("jwt");
  config.auth.jwt->jwks_uri = "file:///jwks.json";
  config.auth.jwt->expected_issuer = "cta";
  config.auth.jwt->expected_audience = "cta-workflow-api";
  EXPECT_FALSE(config.validate().ok());
}

TEST(WorkflowApiConfig, ZeroCatalogueConnectionsIsRejected) {
  auto config = makeValidConfig();
  config.catalogue.client.number_of_connections = 0;
  EXPECT_FALSE(config.validate().ok());
}

#ifdef CTA_PGSCHED
TEST(WorkflowApiConfig, OpportunisticBatchingIsDisabledByDefault) {
  EXPECT_FALSE(makeValidConfig().opportunistic_batching.has_value());
}

TEST(WorkflowApiConfig, BatchingRequiresExperimentalSelection) {
  auto config = makeValidConfig();
  config.opportunistic_batching.emplace();
  config.enabled_features = {"opportunistic_batching"};
  EXPECT_FALSE(config.featureEnabled(cta::workflow_api::WorkflowApiConfig::Feature::Batching));
  EXPECT_FALSE(config.validate().ok());
  config.enabled_features.clear();
  config.enabled_experimental_features = {"opportunistic_batching"};
  EXPECT_TRUE(config.featureEnabled(cta::workflow_api::WorkflowApiConfig::Feature::Batching));
  EXPECT_TRUE(config.validate().ok());
}

TEST(WorkflowApiConfig, ZeroOpportunisticBatchingMaxBatchSizeIsRejected) {
  auto config = makeValidConfig();
  config.enabled_experimental_features = {"opportunistic_batching"};
  config.opportunistic_batching.emplace().max_batch_size = 0;
  EXPECT_FALSE(config.validate().ok());
}
#endif

using cta::grpc_server::AuthMethod;
using cta::grpc_server::JwtAuthConfig;
using cta::grpc_server::MtlsAuthConfig;
using cta::workflow_api::WfeAuthConfig;

namespace {

JwtAuthConfig makeValidJwtConfig() {
  JwtAuthConfig config;
  config.jwks_uri = "file:///etc/cta/jwks.json";
  config.expected_issuer = "cta";
  config.expected_audience = "cta-admin";
  return config;
}

void checkDefaultJwtAuthentication() {
  WfeAuthConfig config;
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
  const auto loaded = cta::runtime::loadFromTomlString<WfeAuthConfig>(document, "auth.toml", false);
  EXPECT_TRUE(loaded.methodEnabled(AuthMethod::JWT));
  EXPECT_FALSE(loaded.methodEnabled(AuthMethod::MTLS));
  EXPECT_THROW(cta::runtime::loadFromTomlString<WfeAuthConfig>(document, "auth.toml", true), cta::exception::UserError);
  EXPECT_NO_THROW(
    cta::runtime::loadFromTomlString<WfeAuthConfig>("enabled_methods = ['jwt']\n" + document, "auth.toml", true));
}

MtlsAuthConfig makeEnabledMtlsConfig() {
  MtlsAuthConfig config;
  return config;
}

}  // namespace

TEST(WfeAuthConfig, EnumQueriesRespectSupportedMethods) {
  WfeAuthConfig config;
  EXPECT_TRUE(config.methodEnabled(AuthMethod::JWT));
  EXPECT_FALSE(config.methodEnabled(AuthMethod::MTLS));

  config.enabled_methods = {"mtls", "kerberos"};
  EXPECT_FALSE(config.methodEnabled(AuthMethod::JWT));
  EXPECT_TRUE(config.methodEnabled(AuthMethod::MTLS));
  EXPECT_FALSE(config.methodEnabled(AuthMethod::KERBEROS));

  config.enabled_methods.clear();
  EXPECT_FALSE(config.methodEnabled(AuthMethod::MTLS));
  EXPECT_FALSE(config.methodEnabled(AuthMethod::JWT));
}

TEST(WfeAuthConfig, MtlsOnlyIsValid) {
  WfeAuthConfig config;
  config.enabled_methods.clear();
  config.mtls = makeEnabledMtlsConfig();
  config.enabled_methods.push_back("mtls");

  EXPECT_TRUE(config.validate().ok());
  EXPECT_TRUE(config.methodEnabled(AuthMethod::MTLS));
  EXPECT_FALSE(config.methodEnabled(AuthMethod::JWT));
}

TEST(WfeAuthConfig, JwtOnlyIsValid) {
  WfeAuthConfig config;
  config.enabled_methods.clear();
  config.jwt = makeValidJwtConfig();
  config.enabled_methods.push_back("jwt");

  EXPECT_TRUE(config.validate().ok());
  EXPECT_TRUE(config.methodEnabled(AuthMethod::JWT));
  EXPECT_FALSE(config.methodEnabled(AuthMethod::MTLS));
}

TEST(WfeAuthConfig, NoEnabledMethodIsRejected) {
  WfeAuthConfig config;
  config.enabled_methods.clear();
  EXPECT_FALSE(config.validate().ok());
}

TEST(WfeAuthConfig, JwtAndMtlsTogetherIsRejected) {
  WfeAuthConfig config;
  config.enabled_methods.clear();
  config.jwt = makeValidJwtConfig();
  config.enabled_methods.push_back("jwt");
  config.mtls = makeEnabledMtlsConfig();
  config.enabled_methods.push_back("mtls");

  EXPECT_FALSE(config.validate().ok());
}

TEST(WfeAuthConfig, MtlsRejectsEmptyAliasIdentity) {
  WfeAuthConfig config;
  config.enabled_methods.clear();
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

TEST(WfeAuthConfig, DefaultsToJwtButStrictModeRequiresExplicitSelection) {
  checkDefaultJwtAuthentication();
}

TEST(WfeAuthConfig, InactiveJwtSettingsCannotEnableJwtAuthentication) {
  for (bool strict : {false, true}) {
    const std::string base = std::string("enabled_methods = ['mtls']\n[mtls]\naliases = {}\n");
    EXPECT_THROW(cta::runtime::loadFromTomlString<WfeAuthConfig>(base + "\n[jwt]\njwks_uri = 42", "auth.toml", strict),
                 cta::exception::UserError);
    const auto config = cta::runtime::loadFromTomlString<WfeAuthConfig>(base + R"toml(
[jwt]
jwks_uri = ""
expected_issuer = ""
expected_audience = ""
min_generation = 0
cache_refresh_interval = 0
pub_key_timeout = 0
jwks_total_timeout = 0
)toml",
                                                                        "auth.toml",
                                                                        strict);
    ASSERT_TRUE(config.jwt);
    EXPECT_TRUE(config.methodEnabled(AuthMethod::MTLS));
    EXPECT_FALSE(config.methodEnabled(AuthMethod::JWT));
  }
}

}  // namespace unitTests
