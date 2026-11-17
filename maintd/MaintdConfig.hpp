/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "runtime/config/CommonConfig.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
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

/// Routines available with the configured scheduler backend.
enum class Routine {
  DiskReportArchive,
  DiskReportRetrieve,
  RepackExpand,
  RepackReport,
#ifndef CTA_PGSCHED
  GarbageCollect,
  QueueCleanup,
#else
  UserActiveQueueCleanup,
  RepackActiveQueueCleanup,
  UserPendingQueueCleanup,
  RepackPendingQueueCleanup,
  SchedulerMaintenanceCleanup,
#endif
};

/// Maintenance routine selection, cycle timing and per-routine settings.
struct RoutinesConfig final {
  // All declared routines run unless explicitly deselected.
  std::vector<std::string> enabled_routines = cta::runtime::featureNames(routines);
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

  static constexpr auto diskReportArchive =
    cta::runtime::stableFeature("disk_report_archive", &RoutinesConfig::disk_report_archive);
  static constexpr auto diskReportRetrieve =
    cta::runtime::stableFeature("disk_report_retrieve", &RoutinesConfig::disk_report_retrieve);
  static constexpr auto repackExpand = cta::runtime::stableFeature("repack_expand", &RoutinesConfig::repack_expand);
  static constexpr auto repackReport = cta::runtime::stableFeature("repack_report", &RoutinesConfig::repack_report);
#ifndef CTA_PGSCHED
  static constexpr auto garbageCollect = cta::runtime::stableFeature("garbage_collect");
  static constexpr auto queueCleanup = cta::runtime::stableFeature("queue_cleanup", &RoutinesConfig::queue_cleanup);
#else
  static constexpr auto userActiveQueueCleanup =
    cta::runtime::stableFeature("user_active_queue_cleanup", &RoutinesConfig::user_active_queue_cleanup);
  static constexpr auto repackActiveQueueCleanup =
    cta::runtime::stableFeature("repack_active_queue_cleanup", &RoutinesConfig::repack_active_queue_cleanup);
  static constexpr auto userPendingQueueCleanup =
    cta::runtime::stableFeature("user_pending_queue_cleanup", &RoutinesConfig::user_pending_queue_cleanup);
  static constexpr auto repackPendingQueueCleanup =
    cta::runtime::stableFeature("repack_pending_queue_cleanup", &RoutinesConfig::repack_pending_queue_cleanup);
  static constexpr auto schedulerMaintenanceCleanup =
    cta::runtime::stableFeature("scheduler_maintenance_cleanup", &RoutinesConfig::scheduler_maintenance_cleanup);
#endif

  static constexpr auto routines = std::tuple {diskReportArchive,
                                               diskReportRetrieve,
                                               repackExpand,
                                               repackReport,
#ifndef CTA_PGSCHED
                                               garbageCollect,
                                               queueCleanup
#else
                                               userActiveQueueCleanup,
                                               repackActiveQueueCleanup,
                                               userPendingQueueCleanup,
                                               repackPendingQueueCleanup,
                                               schedulerMaintenanceCleanup
#endif
  };

  /// Return whether the routine is selected, independently of its settings section.
  bool routineEnabled(Routine routine) const {
    switch (routine) {
      case Routine::DiskReportArchive:
        return cta::runtime::featureEnabled(enabled_routines, diskReportArchive);
      case Routine::DiskReportRetrieve:
        return cta::runtime::featureEnabled(enabled_routines, diskReportRetrieve);
      case Routine::RepackExpand:
        return cta::runtime::featureEnabled(enabled_routines, repackExpand);
      case Routine::RepackReport:
        return cta::runtime::featureEnabled(enabled_routines, repackReport);
#ifndef CTA_PGSCHED
      case Routine::GarbageCollect:
        return cta::runtime::featureEnabled(enabled_routines, garbageCollect);
      case Routine::QueueCleanup:
        return cta::runtime::featureEnabled(enabled_routines, queueCleanup);
#else
      case Routine::UserActiveQueueCleanup:
        return cta::runtime::featureEnabled(enabled_routines, userActiveQueueCleanup);
      case Routine::RepackActiveQueueCleanup:
        return cta::runtime::featureEnabled(enabled_routines, repackActiveQueueCleanup);
      case Routine::UserPendingQueueCleanup:
        return cta::runtime::featureEnabled(enabled_routines, userPendingQueueCleanup);
      case Routine::RepackPendingQueueCleanup:
        return cta::runtime::featureEnabled(enabled_routines, repackPendingQueueCleanup);
      case Routine::SchedulerMaintenanceCleanup:
        return cta::runtime::featureEnabled(enabled_routines, schedulerMaintenanceCleanup);
#endif
    }
    return false;
  }

  /// Identify active sections so the loader can apply strictness and retain defaults for selected routines.
  bool isSectionEnabled(std::string_view name) const { return cta::runtime::featureEnabled(enabled_routines, name); }

  cta::runtime::ValidationResult validate() const {
    auto result = cta::runtime::validateFeatureSelection(*this, enabled_routines, routines, "enabled_routines");
    if (cycle_sleep_interval_secs == 0) {
      result.addError("cycle_sleep_interval_secs", "must be greater than zero");
    }
    if (max_cycle_duration_secs == 0) {
      result.addError("max_cycle_duration_secs", "must be greater than zero");
    }
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

  static constexpr auto telemetryFeature = cta::runtime::experimentalFeature("telemetry", &MaintdConfig::telemetry);
  static constexpr auto healthServerFeature =
    cta::runtime::stableFeature("health_server", &MaintdConfig::health_server);

  static constexpr auto features = std::tuple {telemetryFeature, healthServerFeature};

  bool isTelemetryEnabled() const { return cta::runtime::featureEnabled(*this, telemetryFeature); }

  bool isHealthServerEnabled() const { return cta::runtime::featureEnabled(*this, healthServerFeature); }

  cta::runtime::ValidationResult validate() const {
    auto result = cta::runtime::validateFeatures(*this);
    result.merge("catalogue", catalogue.validate());
    result.merge("scheduler", scheduler.validate());
    result.merge("logging", logging.validate());
    result.merge("xrootd", xrootd.validate());
    result.merge("routines", routines.validate());
    return result;
  }
};

}  // namespace cta::maintd
