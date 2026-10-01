/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapedConfig.hpp"

#include "runtime/config/parsing/TomlParser.hpp"

#include <gtest/gtest.h>

namespace cta::tape::daemon {
TEST(TapedConfigTest, PreparationAndFinalizationTimeoutDefaultsAndValidation) {
  MountsConfig config;
  EXPECT_EQ(900, config.preparing_timeout_secs);
  EXPECT_EQ(900, config.finalizing_timeout_secs);
  EXPECT_TRUE(config.validate().ok());
  config.preparing_timeout_secs = 0;
  EXPECT_FALSE(config.validate().ok());
  EXPECT_NE(std::string::npos, config.validate().what().find("preparing_timeout_secs"));
  config.preparing_timeout_secs = 1;
  config.finalizing_timeout_secs = 0;
  EXPECT_FALSE(config.validate().ok());
  EXPECT_NE(std::string::npos, config.validate().what().find("finalizing_timeout_secs"));
  config.finalizing_timeout_secs = 1;
  EXPECT_TRUE(config.validate().ok());
}

TEST(TapedConfigTest, PreparationAndFinalizationTimeoutOverrides) {
  MountsConfig config;
  EXPECT_TRUE(runtime::parsing::parseTable(config, toml::parse(""), false).ok());
  EXPECT_EQ(900, config.preparing_timeout_secs);
  EXPECT_EQ(900, config.finalizing_timeout_secs);
  const auto table = toml::parse("preparing_timeout_secs = 30\nfinalizing_timeout_secs = 60");
  EXPECT_TRUE(runtime::parsing::parseTable(config, table, false).ok());
  EXPECT_EQ(30, config.preparing_timeout_secs);
  EXPECT_EQ(60, config.finalizing_timeout_secs);
}
}  // namespace cta::tape::daemon
