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
cta::runtime::CatalogueConfig makeMissingCatalogueConfig() {
  cta::runtime::CatalogueConfig config;
  config.config_file = "/nonexistent/cta-service-resources-test/cta-catalogue.conf";
  return config;
}

}  // namespace

TEST(ServiceResourcesTest, throwsWhenCatalogueConfigFileIsMissing) {
  cta::log::DummyLogger log("dummyHost", "ServiceResourcesTest");
  const cta::runtime::SchedulerConfig schedulerConfig;
  const cta::service_resources::CatalogueConfig catalogueOptions;
  const cta::service_resources::FilesConfig filesOptions;

  const auto create = [&] {
#ifndef CTA_PGSCHED
    cta::service_resources::SchedulerConfig schedulerOptions;
    schedulerOptions.number_of_threads = 1;
    return cta::service_resources::ServiceResources(makeMissingCatalogueConfig(),
                                                    schedulerConfig,
                                                    catalogueOptions,
                                                    filesOptions,
                                                    schedulerOptions,
                                                    log);
#else
    return cta::service_resources::ServiceResources(makeMissingCatalogueConfig(),
                                                    schedulerConfig,
                                                    catalogueOptions,
                                                    filesOptions,
                                                    log);
#endif
  };
  EXPECT_THROW(create(), cta::exception::Exception);
}

}  // namespace unitTests
