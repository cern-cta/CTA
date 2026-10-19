/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "grpc-server/server/ServiceContext.hpp"

#include "common/exception/Exception.hpp"
#include "common/log/DummyLogger.hpp"

#include <gtest/gtest.h>

namespace unitTests {

namespace {

// Full initialisation needs a real Catalogue and Scheduler DB, which are covered by the system tests.
// Here we check that the context refuses to start with an unusable configuration.
cta::runtime::CatalogueConfig makeMissingCatalogueConfig() {
  cta::runtime::CatalogueConfig config;
  config.config_file = "/nonexistent/cta-grpc-server-context-test/cta-catalogue.conf";
  return config;
}

}  // namespace

TEST(ServiceContextTest, throwsWhenCatalogueConfigFileIsMissing) {
  cta::log::DummyLogger log("dummyHost", "ServiceContextTest");
  const cta::runtime::SchedulerConfig schedulerConfig;
  const cta::grpc_server::CatalogueConfig catalogueOptions;
  const cta::grpc_server::FilesConfig filesOptions;

  const auto create = [&] {
#ifndef CTA_PGSCHED
    cta::grpc_server::SchedulerConfig schedulerOptions;
    schedulerOptions.number_of_threads = 1;
    return cta::grpc_server::ServiceContext(makeMissingCatalogueConfig(),
                                            schedulerConfig,
                                            catalogueOptions,
                                            filesOptions,
                                            schedulerOptions,
                                            log);
#else
    return cta::grpc_server::ServiceContext(makeMissingCatalogueConfig(),
                                            schedulerConfig,
                                            catalogueOptions,
                                            filesOptions,
                                            log);
#endif
  };
  EXPECT_THROW(create(), cta::exception::Exception);
}

}  // namespace unitTests
