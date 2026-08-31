/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MaintdConfig.hpp"

#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <vector>

namespace unitTests {

TEST(MaintdConfig, DefaultValuesAreValid) {
  EXPECT_TRUE(cta::maintd::MaintdConfig {}.validate().ok());
}

TEST(MaintdConfig, ValidatesCommonConfigChildren) {
  cta::maintd::MaintdConfig config;
  config.logging.level = "invalid";

  EXPECT_FALSE(config.validate().ok());
}

TEST(MaintdConfig, ValidatesRoutinesConfig) {
  cta::maintd::MaintdConfig config;
  config.routines.max_cycle_duration_secs = 0;

  EXPECT_FALSE(config.validate().ok());
}

TEST(MaintdConfig, RoutinesConfigValidatesChildren) {
  cta::maintd::MaintdConfig config;
  config.routines.disk_report_archive.batch_size = 0;

  EXPECT_FALSE(config.validate().ok());
}

TEST(MaintdConfig, ReportsErrorsFromAllInvalidChildren) {
  cta::maintd::MaintdConfig config;
  config.catalogue.config_file.clear();
  config.scheduler.config_file.clear();
  config.logging.level = "invalid";
  config.telemetry.on_init_failure = "ignore";
  config.health_server.enabled = true;
  config.health_server.host = std::nullopt;
  config.health_server.port = std::nullopt;
  config.xrootd.security_protocol.clear();
  config.routines.cycle_sleep_interval_secs = 0;
  config.routines.disk_report_archive.batch_size = 0;
  config.routines.disk_report_retrieve.soft_timeout_secs = 0;
  config.routines.repack_expand.max_to_expand = 0;
  config.routines.repack_report.soft_timeout_secs = 0;
#ifndef CTA_PGSCHED
  config.routines.queue_cleanup.batch_size = 0;
#else
  config.routines.user_active_queue_cleanup.batch_size = 0;
  config.routines.repack_active_queue_cleanup.age_for_collection_secs = 0;
  config.routines.user_pending_queue_cleanup.batch_size = 0;
  config.routines.repack_pending_queue_cleanup.age_for_collection_secs = 0;
  config.routines.scheduler_maintenance_cleanup.age_for_deletion_secs = 0;
#endif

  const auto message = config.validate().what();
  const std::vector<std::string> expectedFields = {
    "catalogue.config_file",
    "health_server.host",
    "health_server.port",
    "logging.level",
    "routines.cycle_sleep_interval_secs",
    "routines.disk_report_archive.batch_size",
    "routines.disk_report_retrieve.soft_timeout_secs",
    "routines.repack_expand.max_to_expand",
    "routines.repack_report.soft_timeout_secs",
    "scheduler.config_file",
    "telemetry.on_init_failure",
    "xrootd.security_protocol",
#ifndef CTA_PGSCHED
    "routines.queue_cleanup.batch_size",
#else
    "routines.repack_active_queue_cleanup.age_for_collection_secs",
    "routines.repack_pending_queue_cleanup.age_for_collection_secs",
    "routines.scheduler_maintenance_cleanup.age_for_deletion_secs",
    "routines.user_active_queue_cleanup.batch_size",
    "routines.user_pending_queue_cleanup.batch_size",
#endif
  };

  for (const auto& field : expectedFields) {
    EXPECT_NE(message.find("Field '" + field + "'"), std::string::npos) << field;
  }
}

}  // namespace unitTests
