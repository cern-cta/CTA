/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapedConfig.hpp"

#include "runtime/config/parsing/TomlParser.hpp"

#include <gtest/gtest.h>

namespace cta::tape::daemon {
TEST(TapedConfigTest, DrivePreparationDefaultsAndOverrides) {
  TapedConfig defaults;
  EXPECT_TRUE(runtime::parsing::parseTable(defaults, toml::parse(""), false).ok());
  EXPECT_FALSE(defaults.drive.clean_on_up);
  EXPECT_FALSE(defaults.drive.startup.recover_existing_up);
  EXPECT_FALSE(defaults.drive.startup.auto_up);

  for (const bool clean : {false, true}) {
    for (const bool recover : {false, true}) {
      for (const bool autoUp : {false, true}) {
        TapedConfig config;
        const auto table = toml::parse(std::string("[drive]\nclean_on_up = ") + (clean ? "true" : "false")
                                       + "\n[drive.startup]\nrecover_existing_up = " + (recover ? "true" : "false")
                                       + "\nauto_up = " + (autoUp ? "true" : "false"));
        ASSERT_TRUE(runtime::parsing::parseTable(config, table, false).ok());
        EXPECT_EQ(clean, config.drive.clean_on_up);
        EXPECT_EQ(recover, config.drive.startup.recover_existing_up);
        EXPECT_EQ(autoUp, config.drive.startup.auto_up);
      }
    }
  }
}

TEST(TapedConfigTest, DrivePreparationRejectsNonBooleanOptions) {
  for (const auto* entry :
       {"[drive]\nclean_on_up = 1", "[drive.startup]\nrecover_existing_up = 'true'", "[drive.startup]\nauto_up = 1"}) {
    TapedConfig config;
    EXPECT_FALSE(runtime::parsing::parseTable(config, toml::parse(entry), false).ok());
  }
}

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

TEST(TapedConfigTest, UnloadTimeoutDefaultsAndValidation) {
  TapedConfig config;
  EXPECT_EQ(900, config.mounts.tape_unload_timeout_secs);
  EXPECT_TRUE(config.mounts.validate().ok());
  config.mounts.tape_unload_timeout_secs = 0;
  EXPECT_FALSE(config.mounts.validate().ok());
  EXPECT_NE(std::string::npos, config.mounts.validate().what().find("tape_unload_timeout_secs"));
  config.mounts.tape_unload_timeout_secs = 1;
  EXPECT_TRUE(config.mounts.validate().ok());
}

}  // namespace cta::tape::daemon
