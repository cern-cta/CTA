/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "runtime/config/CommonConfig.hpp"

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace cta::maintd {

/// Batch size and timing for reporting archive or retrieve results to the disk system.
struct DiskReportRoutineConfig final {
  uint32_t batch_size = 500;
  uint32_t soft_timeout_secs = 30;
  uint32_t iter_sleep_ms = 0;

  static constexpr std::size_t memberCount() { return 3; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (batch_size == 0) {
      result.addError("batch_size", "must be greater than zero");
    }
    if (soft_timeout_secs == 0) {
      result.addError("soft_timeout_secs", "must be greater than zero");
    }
    return result;
  }
};

/// Limit on repack requests expanded during each maintenance cycle.
struct RepackExpandRoutineConfig final {
  uint32_t max_to_expand = 2;

  static constexpr std::size_t memberCount() { return 1; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (max_to_expand == 0) {
      result.addError("max_to_expand", "must be greater than zero");
    }
    return result;
  }
};

/// Time budget for processing transfer reports from repack requests.
struct RepackReportRoutineConfig final {
  uint32_t soft_timeout_secs = 900;

  static constexpr std::size_t memberCount() { return 1; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (soft_timeout_secs == 0) {
      result.addError("soft_timeout_secs", "must be greater than zero");
    }
    return result;
  }
};

#ifndef CTA_PGSCHED

/// Batch size for moving requests out of objectstore queues marked for cleanup.
struct QueueCleanupRoutineConfig final {
  uint32_t batch_size = 500;

  static constexpr std::size_t memberCount() { return 1; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (batch_size == 0) {
      result.addError("batch_size", "must be greater than zero");
    }
    return result;
  }
};

#else

/// Batch size and age threshold for collecting PostgreSQL active or pending queue entries.
struct ActivePendingQueueCleanupRoutineConfig final {
  uint32_t batch_size = 500;
  uint32_t age_for_collection_secs = 900;

  static constexpr std::size_t memberCount() { return 2; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (batch_size == 0) {
      result.addError("batch_size", "must be greater than zero");
    }
    if (age_for_collection_secs == 0) {
      result.addError("age_for_collection_secs", "must be greater than zero");
    }
    return result;
  }
};

/// Batch size and retention period for cleaning PostgreSQL scheduler maintenance entries.
struct SchedulerMaintenanceCleanupRoutineConfig final {
  uint32_t batch_size = 500;
  uint32_t age_for_deletion_secs = 1209600;

  static constexpr std::size_t memberCount() { return 2; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (batch_size == 0) {
      result.addError("batch_size", "must be greater than zero");
    }
    if (age_for_deletion_secs == 0) {
      result.addError("age_for_deletion_secs", "must be greater than zero");
    }
    return result;
  }
};

#endif

/// Maintenance routine selection, cycle timing and per-routine settings.
struct RoutinesConfig final {
  // All routines supported by this scheduler backend run unless explicitly deselected.
#ifndef CTA_PGSCHED
  std::vector<std::string> enabled_routines = {"disk_report_archive",
                                               "disk_report_retrieve",
                                               "repack_expand",
                                               "repack_report",
                                               "garbage_collect",
                                               "queue_cleanup"};
#else
  std::vector<std::string> enabled_routines = {"disk_report_archive",
                                               "disk_report_retrieve",
                                               "repack_expand",
                                               "repack_report",
                                               "user_active_queue_cleanup",
                                               "repack_active_queue_cleanup",
                                               "user_pending_queue_cleanup",
                                               "repack_pending_queue_cleanup",
                                               "scheduler_maintenance_cleanup"};
#endif
  uint32_t cycle_sleep_interval_secs = 10;
  uint32_t max_cycle_duration_secs = 900;

  std::optional<DiskReportRoutineConfig> disk_report_archive = DiskReportRoutineConfig {};
  std::optional<DiskReportRoutineConfig> disk_report_retrieve = DiskReportRoutineConfig {};

  std::optional<RepackExpandRoutineConfig> repack_expand = RepackExpandRoutineConfig {};
  std::optional<RepackReportRoutineConfig> repack_report = RepackReportRoutineConfig {};

#ifndef CTA_PGSCHED
  std::optional<QueueCleanupRoutineConfig> queue_cleanup = QueueCleanupRoutineConfig {};

  static constexpr std::size_t memberCount() { return 8; }
#else
  std::optional<ActivePendingQueueCleanupRoutineConfig> user_active_queue_cleanup =
    ActivePendingQueueCleanupRoutineConfig {};
  std::optional<ActivePendingQueueCleanupRoutineConfig> repack_active_queue_cleanup =
    ActivePendingQueueCleanupRoutineConfig {};
  std::optional<ActivePendingQueueCleanupRoutineConfig> user_pending_queue_cleanup =
    ActivePendingQueueCleanupRoutineConfig {};
  std::optional<ActivePendingQueueCleanupRoutineConfig> repack_pending_queue_cleanup =
    ActivePendingQueueCleanupRoutineConfig {};
  std::optional<SchedulerMaintenanceCleanupRoutineConfig> scheduler_maintenance_cleanup =
    SchedulerMaintenanceCleanupRoutineConfig {};

  static constexpr std::size_t memberCount() { return 12; }
#endif

  /// The loader retains defaults only for selected routines and rejects supplied inactive sections.
  bool isSectionEnabled(std::string_view name) const { return cta::runtime::featureEnabled(enabled_routines, name); }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
#ifndef CTA_PGSCHED
    result.merge("",
                 cta::runtime::validateFeatureNames(enabled_routines,
                                                    {"disk_report_archive",
                                                     "disk_report_retrieve",
                                                     "repack_expand",
                                                     "repack_report",
                                                     "garbage_collect",
                                                     "queue_cleanup"},
                                                    "enabled_routines"));
#else
    result.merge("",
                 cta::runtime::validateFeatureNames(enabled_routines,
                                                    {"disk_report_archive",
                                                     "disk_report_retrieve",
                                                     "repack_expand",
                                                     "repack_report",
                                                     "user_active_queue_cleanup",
                                                     "repack_active_queue_cleanup",
                                                     "user_pending_queue_cleanup",
                                                     "repack_pending_queue_cleanup",
                                                     "scheduler_maintenance_cleanup"},
                                                    "enabled_routines"));
#endif
    if (cycle_sleep_interval_secs == 0) {
      result.addError("cycle_sleep_interval_secs", "must be greater than zero");
    }
    if (max_cycle_duration_secs == 0) {
      result.addError("max_cycle_duration_secs", "must be greater than zero");
    }
    cta::runtime::validateFeature(result, enabled_routines, "disk_report_archive", disk_report_archive);
    cta::runtime::validateFeature(result, enabled_routines, "disk_report_retrieve", disk_report_retrieve);
    cta::runtime::validateFeature(result, enabled_routines, "repack_expand", repack_expand);
    cta::runtime::validateFeature(result, enabled_routines, "repack_report", repack_report);
#ifndef CTA_PGSCHED

    cta::runtime::validateFeature(result, enabled_routines, "queue_cleanup", queue_cleanup);
#else
    cta::runtime::validateFeature(result, enabled_routines, "user_active_queue_cleanup", user_active_queue_cleanup);
    cta::runtime::validateFeature(result, enabled_routines, "repack_active_queue_cleanup", repack_active_queue_cleanup);
    cta::runtime::validateFeature(result, enabled_routines, "user_pending_queue_cleanup", user_pending_queue_cleanup);
    cta::runtime::validateFeature(result,
                                  enabled_routines,
                                  "repack_pending_queue_cleanup",
                                  repack_pending_queue_cleanup);
    cta::runtime::validateFeature(result,
                                  enabled_routines,
                                  "scheduler_maintenance_cleanup",
                                  scheduler_maintenance_cleanup);
#endif
    return result;
  }
};

/// Catalogue, scheduler, monitoring and routine settings for the maintenance daemon.
struct MaintdConfig final {
  cta::runtime::CatalogueConfig catalogue;
  cta::runtime::SchedulerConfig scheduler;
  cta::runtime::LoggingConfig logging;
  std::optional<cta::runtime::TelemetryConfig> telemetry = std::nullopt;
  std::optional<cta::runtime::HealthServerConfig> health_server = std::nullopt;
  std::vector<std::string> enabled_features;
  std::vector<std::string> enabled_experimental_features;
  cta::runtime::XRootDConfig xrootd;
  RoutinesConfig routines;

  static constexpr std::size_t memberCount() { return 9; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    result.merge("catalogue", catalogue.validate());
    result.merge("scheduler", scheduler.validate());
    result.merge("logging", logging.validate());
    cta::runtime::validateFeature(result, enabled_experimental_features, "telemetry", telemetry);
    cta::runtime::validateFeature(result, enabled_features, "health_server", health_server);
    result.merge("",
                 cta::runtime::validateFeatureLists(enabled_features,
                                                    enabled_experimental_features,
                                                    {"health_server"},
                                                    {"telemetry"}));
    result.merge("xrootd", xrootd.validate());
    result.merge("routines", routines.validate());
    return result;
  }
};

}  // namespace cta::maintd
