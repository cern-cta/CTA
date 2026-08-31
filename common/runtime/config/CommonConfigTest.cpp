/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "CommonConfig.hpp"

#include <gtest/gtest.h>

namespace unitTests {

TEST(CommonConfig, DefaultValuesAreValid) {
  EXPECT_NO_THROW(cta::runtime::ExperimentalConfig {}.validate());
  EXPECT_NO_THROW(cta::runtime::CatalogueConfig {}.validate());
  EXPECT_NO_THROW(cta::runtime::SchedulerConfig {}.validate());
  EXPECT_NO_THROW(cta::runtime::LoggingConfig {}.validate());
  EXPECT_NO_THROW(cta::runtime::TelemetryConfig {}.validate());
  EXPECT_NO_THROW(cta::runtime::HealthServerConfig {}.validate());
  EXPECT_NO_THROW(cta::runtime::XRootDConfig {}.validate());
}

TEST(CommonConfig, RejectsInvalidStringChoice) {
  cta::runtime::LoggingConfig config;
  config.level = "verbose";

  EXPECT_THROW(config.validate(), cta::exception::UserError);
}

TEST(CommonConfig, RejectsEmptyRequiredString) {
  cta::runtime::CatalogueConfig config;
  config.config_file.clear();

  EXPECT_THROW(config.validate(), cta::exception::UserError);
}

TEST(CommonConfig, RejectsNonPositiveValue) {
  cta::runtime::SchedulerConfig config;
  config.tape_cache_max_age_secs = 0;

  EXPECT_THROW(config.validate(), cta::exception::UserError);
}

}  // namespace unitTests
