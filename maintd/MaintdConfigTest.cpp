/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MaintdConfig.hpp"

#include <gtest/gtest.h>

namespace unitTests {

TEST(MaintdConfig, DefaultValuesAreValid) {
  EXPECT_NO_THROW(cta::maintd::MaintdConfig {}.validate());
}

TEST(MaintdConfig, ValidatesCommonConfigChildren) {
  cta::maintd::MaintdConfig config;
  config.logging.level = "invalid";

  EXPECT_THROW(config.validate(), cta::exception::UserError);
}

TEST(MaintdConfig, ValidatesRoutinesConfig) {
  cta::maintd::MaintdConfig config;
  config.routines.max_cycle_duration_secs = 0;

  EXPECT_THROW(config.validate(), cta::exception::UserError);
}

TEST(MaintdConfig, RoutinesConfigValidatesChildren) {
  cta::maintd::MaintdConfig config;
  config.routines.disk_report_archive.batch_size = 0;

  EXPECT_THROW(config.validate(), cta::exception::UserError);
}

}  // namespace unitTests
