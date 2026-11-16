/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "AdminApiConfig.hpp"

#include "common/exception/UserError.hpp"
#include "runtime/config/ConfigLoader.hpp"

#include <gtest/gtest.h>
#include <string>
#include <toml++/toml.hpp>
#include <utility>

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
}  // namespace unitTests
