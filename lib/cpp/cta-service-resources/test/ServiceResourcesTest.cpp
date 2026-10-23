/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "service-resources/ServiceResources.hpp"

#include "common/exception/Exception.hpp"
#include "common/log/DummyLogger.hpp"

#include <gtest/gtest.h>

namespace unitTests {

namespace {

// Full initialisation needs a real Catalogue and Scheduler DB, which are covered by the system tests.
// Here we check that the context refuses to start with an unusable configuration.
cta::service_resources::ServiceCatalogueConfig makeMissingCatalogueConfig() {
  cta::service_resources::ServiceCatalogueConfig config;
  config.config_file = "/nonexistent/cta-service-resources-test/cta-catalogue.conf";
  return config;
}

}  // namespace

TEST(ServiceResourcesTest, throwsWhenCatalogueConfigFileIsMissing) {
  cta::log::DummyLogger log("dummyHost", "ServiceResourcesTest");
  cta::service_resources::ServiceSchedulerConfig schedulerConfig;
#ifndef CTA_PGSCHED
  schedulerConfig.client.number_of_threads = 1;
#endif
  const auto catalogueConfig = makeMissingCatalogueConfig();

  const auto create = [&] {
    return cta::service_resources::ServiceResources(
      "ServiceResourcesTest",
      catalogueConfig,
      schedulerConfig,
      cta::service_resources::makeConnectionOptions(catalogueConfig, schedulerConfig),
      log);
  };
  EXPECT_THROW(create(), cta::exception::Exception);
}

}  // namespace unitTests
