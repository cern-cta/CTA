/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MaintdConfig.hpp"

#include "runtime/config/ConfigLoader.hpp"

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
  config.routines.disk_report_archive.emplace().batch_size = 0;

  EXPECT_FALSE(config.validate().ok());
}

TEST(MaintdConfig, ReportsErrorsFromAllInvalidChildren) {
  cta::maintd::MaintdConfig config;
  config.catalogue.config_file.clear();
  config.scheduler.config_file.clear();
  config.logging.level = "invalid";
  config.telemetry.emplace().on_init_failure = "ignore";
  config.enabled_features = {"health_server"};
  config.enabled_experimental_features = {"telemetry"};
  config.health_server.emplace().tcp.emplace().host.clear();
  config.health_server->tcp->port = 0;
  config.xrootd.security_protocol.clear();
  config.routines.cycle_sleep_interval_secs = 0;
  config.routines.disk_report_archive.emplace().batch_size = 0;
  config.routines.disk_report_retrieve.emplace().soft_timeout_secs = 0;
  config.routines.repack_expand.emplace().max_to_expand = 0;
  config.routines.repack_report.emplace().soft_timeout_secs = 0;
#ifndef CTA_PGSCHED
  config.routines.queue_cleanup.emplace().batch_size = 0;
#else
  config.routines.user_active_queue_cleanup.emplace().batch_size = 0;
  config.routines.repack_active_queue_cleanup.emplace().age_for_collection_secs = 0;
  config.routines.user_pending_queue_cleanup.emplace().batch_size = 0;
  config.routines.repack_pending_queue_cleanup.emplace().age_for_collection_secs = 0;
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

namespace unitTests {
namespace {
std::string completeRoutinesToml() {
  std::string text = R"toml(
cycle_sleep_interval_secs = 10
max_cycle_duration_secs = 900
[disk_report_archive]
batch_size = 500
soft_timeout_secs = 30
iter_sleep_ms = 0
[disk_report_retrieve]
batch_size = 500
soft_timeout_secs = 30
iter_sleep_ms = 0
[repack_expand]
max_to_expand = 2
[repack_report]
soft_timeout_secs = 900
)toml";
#ifndef CTA_PGSCHED
  text += "\n[queue_cleanup]\nbatch_size = 500\n";
#else
  for (const auto& name : {"user_active_queue_cleanup",
                           "repack_active_queue_cleanup",
                           "user_pending_queue_cleanup",
                           "repack_pending_queue_cleanup"}) {
    text += "\n[" + std::string(name) + "]\nbatch_size = 500\nage_for_collection_secs = 900\n";
  }
  text += "\n[scheduler_maintenance_cleanup]\nbatch_size = 500\nage_for_deletion_secs = 1209600\n";
#endif
  std::string selection = "enabled_routines = [";
  for (const auto& name : cta::maintd::RoutinesConfig {}.enabled_routines) {
    selection += "'" + name + "',";
  }
  return selection + "]\n" + text;
}
}  // namespace

TEST(MaintdConfig, AllSupportedRoutinesAreEnabledByDefault) {
  using cta::maintd::RoutinesConfig;
  const auto config = cta::runtime::loadFromTomlString<RoutinesConfig>("", "routines.toml", false);
  EXPECT_TRUE(config.validate().ok());
  ASSERT_TRUE(config.disk_report_archive);
  ASSERT_TRUE(config.disk_report_retrieve);
  ASSERT_TRUE(config.repack_expand);
  ASSERT_TRUE(config.repack_report);
  EXPECT_EQ(config.disk_report_archive->batch_size, 500);
  EXPECT_EQ(config.repack_report->soft_timeout_secs, 900);
#ifndef CTA_PGSCHED
  EXPECT_EQ(config.enabled_routines.size(), 6);
  EXPECT_TRUE(config.queue_cleanup);
  EXPECT_TRUE(cta::runtime::featureEnabled(config.enabled_routines, "garbage_collect"));
#else
  EXPECT_EQ(config.enabled_routines.size(), 9);
  EXPECT_TRUE(config.user_active_queue_cleanup);
  EXPECT_TRUE(config.repack_active_queue_cleanup);
  EXPECT_TRUE(config.user_pending_queue_cleanup);
  EXPECT_TRUE(config.repack_pending_queue_cleanup);
  EXPECT_TRUE(config.scheduler_maintenance_cleanup);
#endif
  const auto app = cta::runtime::loadFromTomlString<cta::maintd::MaintdConfig>("[scheduler]\nbackend_name = 'test'",
                                                                               "maintd.toml",
                                                                               false);
  EXPECT_EQ(app.routines.enabled_routines, config.enabled_routines);
  EXPECT_TRUE(app.routines.repack_report);
}

TEST(MaintdConfig, RoutineSubsetKeepsOnlySelectedDefaults) {
  using cta::maintd::RoutinesConfig;
  const auto config =
    cta::runtime::loadFromTomlString<RoutinesConfig>("enabled_routines = ['repack_expand']", "routines.toml", false);
  ASSERT_TRUE(config.repack_expand);
  EXPECT_EQ(config.repack_expand->max_to_expand, 2);
  EXPECT_FALSE(config.disk_report_archive);
  EXPECT_FALSE(config.disk_report_retrieve);
  EXPECT_FALSE(config.repack_report);
  EXPECT_TRUE(config.validate().ok());
  const auto disabled =
    cta::runtime::loadFromTomlString<RoutinesConfig>("enabled_routines = []", "routines.toml", false);
  EXPECT_FALSE(disabled.repack_expand);
  EXPECT_TRUE(disabled.validate().ok());
}

TEST(MaintdConfig, RoutineSelectionsAndInactiveSectionsAreValidatedInBothModes) {
  using cta::maintd::RoutinesConfig;
  for (bool strict : {false, true}) {
    const std::string timing = "\ncycle_sleep_interval_secs = 10\nmax_cycle_duration_secs = 900\n";
    EXPECT_NO_THROW(cta::runtime::loadFromTomlString<RoutinesConfig>("enabled_routines = ['repack_expand']" + timing
                                                                       + "[repack_expand]\nmax_to_expand = 3",
                                                                     "routines.toml",
                                                                     strict));
    EXPECT_NO_THROW(
      cta::runtime::loadFromTomlString<RoutinesConfig>("enabled_routines = []" + timing, "routines.toml", strict));
    for (const auto& selection : {"[]", "['unknown']", "['repack_expand', 'repack_expand']"}) {
      EXPECT_THROW(cta::runtime::loadFromTomlString<RoutinesConfig>("enabled_routines = " + std::string(selection)
                                                                      + timing + "[repack_expand]\nmax_to_expand = 2",
                                                                    "routines.toml",
                                                                    strict),
                   cta::exception::UserError);
    }
  }
}

TEST(MaintdConfig, StrictRoutinesRequireExplicitSelectionSectionsAndSettings) {
  using cta::maintd::RoutinesConfig;
  EXPECT_NO_THROW(cta::runtime::loadFromTomlString<RoutinesConfig>(completeRoutinesToml(), "routines.toml", true));
  const std::string timing = "\ncycle_sleep_interval_secs = 10\nmax_cycle_duration_secs = 900\n";
  for (const auto& text : {timing,
                           "enabled_routines = ['repack_expand']" + timing,
                           "enabled_routines = ['repack_expand']" + timing + "[repack_expand]\n"}) {
    EXPECT_THROW(cta::runtime::loadFromTomlString<RoutinesConfig>(text, "routines.toml", true),
                 cta::exception::UserError);
  }
}
}  // namespace unitTests
