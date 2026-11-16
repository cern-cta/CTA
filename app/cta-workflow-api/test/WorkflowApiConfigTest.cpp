/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "WorkflowApiConfig.hpp"

#include "common/exception/UserError.hpp"

#include <gtest/gtest.h>

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
  EXPECT_FALSE(config.validate().ok());
  config.enabled_features.clear();
  config.enabled_experimental_features = {"opportunistic_batching"};
  EXPECT_TRUE(config.validate().ok());
}

TEST(WorkflowApiConfig, ZeroOpportunisticBatchingMaxBatchSizeIsRejected) {
  auto config = makeValidConfig();
  config.enabled_experimental_features = {"opportunistic_batching"};
  config.opportunistic_batching.emplace().max_batch_size = 0;
  EXPECT_FALSE(config.validate().ok());
}
#endif

}  // namespace unitTests
