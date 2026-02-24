/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/runtime/config/CommonConfig.hpp"

#include <cstdint>
#include <map>
#include <optional>
#include <string>

namespace cta::maintd {
struct DiskReportRoutineConfig {
  bool enabled = true;
  int batch_size = 500;
  int soft_timeout_secs = 30;

  static consteval auto fields() {
    return std::make_tuple(cta::runtime::field("enabled", &DiskReportRoutineConfig::enabled),
                           cta::runtime::field("batch_size", &DiskReportRoutineConfig::batch_size),
                           cta::runtime::field("soft_timeout_secs", &DiskReportRoutineConfig::soft_timeout_secs));
  }
};

struct RepackExpandRoutineConfig {
  bool enabled = true;
  int max_to_expand = 2;

  static consteval auto fields() {
    return std::make_tuple(cta::runtime::field("enabled", &RepackExpandRoutineConfig::enabled),
                           cta::runtime::field("max_to_expand", &RepackExpandRoutineConfig::max_to_expand));
  }
};

struct RepackReportRoutineConfig {
  bool enabled = true;
  int soft_timeout_secs = 900;

  static consteval auto fields() {
    return std::make_tuple(cta::runtime::field("enabled", &RepackReportRoutineConfig::enabled),
                           cta::runtime::field("soft_timeout_secs", &RepackReportRoutineConfig::soft_timeout_secs));
  }
};

#ifndef CTA_PGSCHED
struct QueueCleanupRoutineConfig {
  bool enabled = true;
  int batch_size = 500;

  static consteval auto fields() {
    return std::make_tuple(cta::runtime::field("enabled", &QueueCleanupRoutineConfig::enabled),
                           cta::runtime::field("batch_size", &QueueCleanupRoutineConfig::batch_size));
  }
};

struct GarbageCollectRoutineConfig {
  bool enabled = true;

  static consteval auto fields() {
    return std::make_tuple(cta::runtime::field("enabled", &GarbageCollectRoutineConfig::enabled));
  }
};
#else
struct ActivePendingQueueCleanupRoutineConfig {
  bool enabled = true;
  int batch_size = 500;
  int age_for_collection_secs = 900;

  static consteval auto fields() {
    return std::make_tuple(
      cta::runtime::field("enabled", &ActivePendingQueueCleanupRoutineConfig::enabled),
      cta::runtime::field("batch_size", &ActivePendingQueueCleanupRoutineConfig::batch_size),
      cta::runtime::field("age_for_collection_secs", &ActivePendingQueueCleanupRoutineConfig::age_for_collection_secs));
  }
};

struct SchedulerMaintenanceCleanupRoutineConfig {
  bool enabled = true;
  int batch_size = 500;
  int age_for_deletion_secs = 1209600;

  static consteval auto fields() {
    return std::make_tuple(
      cta::runtime::field("enabled", &SchedulerMaintenanceCleanupRoutineConfig::enabled),
      cta::runtime::field("batch_size", &SchedulerMaintenanceCleanupRoutineConfig::batch_size),
      cta::runtime::field("age_for_deletion_secs", &SchedulerMaintenanceCleanupRoutineConfig::age_for_deletion_secs));
  }
};
#endif

struct RoutinesConfig {
  int cycle_sleep_interval_secs = 10;
  int max_cycle_duration_secs = 900;

  DiskReportRoutineConfig disk_report_archive;
  DiskReportRoutineConfig disk_report_retrieve;

  RepackExpandRoutineConfig repack_expand;
  RepackReportRoutineConfig repack_report;

#ifndef CTA_PGSCHED
  GarbageCollectRoutineConfig garbage_collect;
  QueueCleanupRoutineConfig queue_cleanup;
#else
  ActivePendingQueueCleanupRoutineConfig user_active_queue_cleanup;
  ActivePendingQueueCleanupRoutineConfig repack_active_queue_cleanup;
  ActivePendingQueueCleanupRoutineConfig user_pending_queue_cleanup;
  ActivePendingQueueCleanupRoutineConfig repack_pending_queue_cleanup;
  SchedulerMaintenanceCleanupRoutineConfig scheduler_maintenance_cleanup;
#endif

  static consteval auto fields() {
    return std::make_tuple(cta::runtime::field("cycle_sleep_interval_secs", &RoutinesConfig::cycle_sleep_interval_secs),
                           cta::runtime::field("max_cycle_duration_secs", &RoutinesConfig::max_cycle_duration_secs),
                           cta::runtime::field("disk_report_archive", &RoutinesConfig::disk_report_archive),
                           cta::runtime::field("disk_report_retrieve", &RoutinesConfig::disk_report_retrieve),
                           cta::runtime::field("repack_expand", &RoutinesConfig::repack_expand),
                           cta::runtime::field("repack_report", &RoutinesConfig::repack_report),
#ifndef CTA_PGSCHED
                           cta::runtime::field("garbage_collect", &RoutinesConfig::garbage_collect),
                           cta::runtime::field("queue_cleanup", &RoutinesConfig::queue_cleanup)
#else
                           cta::runtime::field("user_active_queue_cleanup", &RoutinesConfig::user_active_queue_cleanup),
                           cta::runtime::field("repack_active_queue_cleanup",
                                               &RoutinesConfig::repack_active_queue_cleanup),
                           cta::runtime::field("user_pending_queue_cleanup",
                                               &RoutinesConfig::user_pending_queue_cleanup),
                           cta::runtime::field("repack_pending_queue_cleanup",
                                               &RoutinesConfig::repack_pending_queue_cleanup),
                           cta::runtime::field("scheduler_maintenance_cleanup",
                                               &RoutinesConfig::scheduler_maintenance_cleanup)
#endif
    );
  }
};

struct MaintdConfig final {
  cta::runtime::CatalogueConfig catalogue;
  cta::runtime::SchedulerConfig scheduler;
  cta::runtime::LoggingConfig logging;
  cta::runtime::TelemetryConfig telemetry;
  cta::runtime::HealthServerConfig health_server;
  cta::runtime::ExperimentalConfig experimental;
  cta::runtime::XRootDConfig xrootd;
  RoutinesConfig routines;

  static consteval auto fields() {
    return std::make_tuple(cta::runtime::field("catalogue", &MaintdConfig::catalogue),
                           cta::runtime::field("scheduler", &MaintdConfig::scheduler),
                           cta::runtime::field("logging", &MaintdConfig::logging),
                           cta::runtime::field("telemetry", &MaintdConfig::telemetry),
                           cta::runtime::field("health_server", &MaintdConfig::health_server),
                           cta::runtime::field("experimental", &MaintdConfig::experimental),
                           cta::runtime::field("xrootd", &MaintdConfig::xrootd),
                           cta::runtime::field("routines", &MaintdConfig::routines));
  }
};

}  // namespace cta::maintd
