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

struct DiskReportRoutineConfig final {
  bool enabled = true;
  unsigned int batch_size = 500;
  unsigned int soft_timeout_secs = 30;

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

struct RepackExpandRoutineConfig final {
  bool enabled = true;
  unsigned int max_to_expand = 2;

  static constexpr std::size_t memberCount() { return 2; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (max_to_expand == 0) {
      result.addError("max_to_expand", "must be greater than zero");
    }
    return result;
  }
};

struct RepackReportRoutineConfig final {
  bool enabled = true;
  unsigned int soft_timeout_secs = 900;

  static constexpr std::size_t memberCount() { return 2; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (soft_timeout_secs == 0) {
      result.addError("soft_timeout_secs", "must be greater than zero");
    }
    return result;
  }
};

#ifndef CTA_PGSCHED

struct QueueCleanupRoutineConfig final {
  bool enabled = true;
  unsigned int batch_size = 500;

  static constexpr std::size_t memberCount() { return 2; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (batch_size == 0) {
      result.addError("batch_size", "must be greater than zero");
    }
    return result;
  }
};

struct GarbageCollectRoutineConfig final {
  bool enabled = true;

  static constexpr std::size_t memberCount() { return 1; }

  cta::runtime::ValidationResult validate() const { return {}; }
};

#else

struct ActivePendingQueueCleanupRoutineConfig final {
  bool enabled = true;
  unsigned int batch_size = 500;
  unsigned int age_for_collection_secs = 900;

  static constexpr std::size_t memberCount() { return 3; }

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

struct SchedulerMaintenanceCleanupRoutineConfig final {
  bool enabled = true;
  unsigned int batch_size = 500;
  unsigned int age_for_deletion_secs = 1209600;

  static constexpr std::size_t memberCount() { return 3; }

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

struct RoutinesConfig final {
  unsigned int cycle_sleep_interval_secs = 10;
  unsigned int max_cycle_duration_secs = 900;

  DiskReportRoutineConfig disk_report_archive;
  DiskReportRoutineConfig disk_report_retrieve;

  RepackExpandRoutineConfig repack_expand;
  RepackReportRoutineConfig repack_report;

#ifndef CTA_PGSCHED
  GarbageCollectRoutineConfig garbage_collect;
  QueueCleanupRoutineConfig queue_cleanup;

  static constexpr std::size_t memberCount() { return 8; }
#else
  ActivePendingQueueCleanupRoutineConfig user_active_queue_cleanup;
  ActivePendingQueueCleanupRoutineConfig repack_active_queue_cleanup;
  ActivePendingQueueCleanupRoutineConfig user_pending_queue_cleanup;
  ActivePendingQueueCleanupRoutineConfig repack_pending_queue_cleanup;
  SchedulerMaintenanceCleanupRoutineConfig scheduler_maintenance_cleanup;

  static constexpr std::size_t memberCount() { return 11; }
#endif

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (cycle_sleep_interval_secs == 0) {
      result.addError("cycle_sleep_interval_secs", "must be greater than zero");
    }
    if (max_cycle_duration_secs == 0) {
      result.addError("max_cycle_duration_secs", "must be greater than zero");
    }
    result.merge("disk_report_archive", disk_report_archive.validate());
    result.merge("disk_report_retrieve", disk_report_retrieve.validate());
    result.merge("repack_expand", repack_expand.validate());
    result.merge("repack_report", repack_report.validate());
#ifndef CTA_PGSCHED
    result.merge("garbage_collect", garbage_collect.validate());
    result.merge("queue_cleanup", queue_cleanup.validate());
#else
    result.merge("user_active_queue_cleanup", user_active_queue_cleanup.validate());
    result.merge("repack_active_queue_cleanup", repack_active_queue_cleanup.validate());
    result.merge("user_pending_queue_cleanup", user_pending_queue_cleanup.validate());
    result.merge("repack_pending_queue_cleanup", repack_pending_queue_cleanup.validate());
    result.merge("scheduler_maintenance_cleanup", scheduler_maintenance_cleanup.validate());
#endif
    return result;
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

  static constexpr std::size_t memberCount() { return 8; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    result.merge("catalogue", catalogue.validate());
    result.merge("scheduler", scheduler.validate());
    result.merge("logging", logging.validate());
    result.merge("telemetry", telemetry.validate());
    result.merge("health_server", health_server.validate());
    result.merge("experimental", experimental.validate());
    result.merge("xrootd", xrootd.validate());
    result.merge("routines", routines.validate());
    return result;
  }
};

}  // namespace cta::maintd
