/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "CommonConfig.hpp"

#include <gtest/gtest.h>

namespace unitTests {

TEST(CommonConfig, DefaultValuesAreValid) {
  EXPECT_TRUE(cta::runtime::ExperimentalConfig {}.validate().ok());
  EXPECT_TRUE(cta::runtime::CatalogueConfig {}.validate().ok());
  EXPECT_TRUE(cta::runtime::SchedulerConfig {}.validate().ok());
  EXPECT_TRUE(cta::runtime::LoggingConfig {}.validate().ok());
  EXPECT_TRUE(cta::runtime::TelemetryConfig {}.validate().ok());
  EXPECT_TRUE(cta::runtime::HealthServerConfig {}.validate().ok());
  EXPECT_TRUE(cta::runtime::XRootDConfig {}.validate().ok());
}

TEST(CommonConfig, RejectsInvalidStringChoice) {
  cta::runtime::LoggingConfig config;
  config.level = "verbose";

  EXPECT_FALSE(config.validate().ok());
}

TEST(CommonConfig, RejectsEmptyRequiredString) {
  cta::runtime::CatalogueConfig config;
  config.config_file.clear();

  EXPECT_FALSE(config.validate().ok());
}

TEST(CommonConfig, RejectsNonPositiveValue) {
  cta::runtime::SchedulerConfig config;
  config.tape_cache_max_age_secs = 0;

  EXPECT_FALSE(config.validate().ok());
}

TEST(CommonConfig, ReportsAllInvalidFields) {
  cta::runtime::LoggingConfig config;
  config.level = "verbose";
  config.format = "xml";

  EXPECT_EQ(config.validate().what(),
            "1) Field 'format' has unsupported value 'xml'.\n"
            "2) Field 'level' has unsupported value 'verbose'.\n");
}

}  // namespace unitTests
