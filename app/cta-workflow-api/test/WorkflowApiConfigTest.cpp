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
  config.auth.mtls = cta::grpc_server::MtlsAuthConfig {true, {{"instance", {"host.example.org"}}}};
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
  EXPECT_THROW(config.validate(), cta::exception::UserError);
}

TEST(WorkflowApiConfig, JwtAndMtlsTogetherAreRejected) {
  auto config = makeValidConfig();
  config.auth.jwt = cta::grpc_server::JwtAuthConfig {};
  config.auth.jwt->enabled = true;
  config.auth.jwt->jwks_uri = "file:///jwks.json";
  config.auth.jwt->expected_issuer = "cta";
  config.auth.jwt->expected_audience = "cta-workflow-api";
  EXPECT_THROW(config.validate(), cta::exception::UserError);
}

TEST(WorkflowApiConfig, ZeroCatalogueConnectionsIsRejected) {
  auto config = makeValidConfig();
  config.catalogue.client.number_of_connections = 0;
  EXPECT_FALSE(config.validate().ok());
}

}  // namespace unitTests
