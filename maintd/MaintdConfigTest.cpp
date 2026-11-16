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

TEST(MaintdConfig, DefaultValuesAreValidOnceBackendNameIsSet) {
  cta::maintd::MaintdConfig config;
  config.scheduler.backend_name = "vfsTest";

  EXPECT_TRUE(config.validate().ok());
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
  config.routines.enabled_features.push_back("disk_report_archive");
  config.routines.disk_report_archive.emplace().batch_size = 0;

  EXPECT_FALSE(config.validate().ok());
}

TEST(MaintdConfig, ReportsErrorsFromAllInvalidChildren) {
  cta::maintd::MaintdConfig config;
  config.catalogue.config_file.clear();
  config.scheduler.config_file.clear();
  config.logging.level = "invalid";
  config.telemetry.emplace().on_init_failure = "ignore";
  config.enabled_features = {"telemetry", "health_server"};
  config.health_server.emplace().tcp.emplace().host.clear();
  config.health_server->tcp->port = 0;
  config.xrootd.security_protocol.clear();
  config.routines.cycle_sleep_interval_secs = 0;
  config.routines.enabled_features.push_back("disk_report_archive");
  config.routines.disk_report_archive.emplace().batch_size = 0;
  config.routines.enabled_features.push_back("disk_report_retrieve");
  config.routines.disk_report_retrieve.emplace().soft_timeout_secs = 0;
  config.routines.enabled_features.push_back("repack_expand");
  config.routines.repack_expand.emplace().max_to_expand = 0;
  config.routines.enabled_features.push_back("repack_report");
  config.routines.repack_report.emplace().soft_timeout_secs = 0;
#ifndef CTA_PGSCHED
  config.routines.enabled_features.push_back("queue_cleanup");
  config.routines.queue_cleanup.emplace().batch_size = 0;
#else
  config.routines.enabled_features.push_back("user_active_queue_cleanup");
  config.routines.user_active_queue_cleanup.emplace().batch_size = 0;
  config.routines.enabled_features.push_back("repack_active_queue_cleanup");
  config.routines.repack_active_queue_cleanup.emplace().age_for_collection_secs = 0;
  config.routines.enabled_features.push_back("user_pending_queue_cleanup");
  config.routines.user_pending_queue_cleanup.emplace().batch_size = 0;
  config.routines.enabled_features.push_back("repack_pending_queue_cleanup");
  config.routines.repack_pending_queue_cleanup.emplace().age_for_collection_secs = 0;
  config.routines.enabled_features.push_back("scheduler_maintenance_cleanup");
  config.routines.scheduler_maintenance_cleanup.emplace().age_for_deletion_secs = 0;
#endif

  const auto message = config.validate().what();
  const std::vector<std::string> expectedFields = {
    "catalogue.config_file",
    "health_server.tcp.host",
    "health_server.tcp.port",
    "logging.level",
    "routines.cycle_sleep_interval_secs",
    "routines.disk_report_archive.batch_size",
    "routines.disk_report_retrieve.soft_timeout_secs",
    "routines.repack_expand.max_to_expand",
    "routines.repack_report.soft_timeout_secs",
    "scheduler.backend_name",
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
