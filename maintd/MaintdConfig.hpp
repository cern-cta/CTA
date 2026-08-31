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

  void validate() const {
    if (batch_size == 0) {
      throw cta::exception::UserError("Disk report batch_size must be positive");
    }
    if (soft_timeout_secs == 0) {
      throw cta::exception::UserError("Disk report soft_timeout_secs must be positive");
    }
  }
};

struct RepackExpandRoutineConfig final {
  bool enabled = true;
  unsigned int max_to_expand = 2;

  static constexpr std::size_t memberCount() { return 2; }

  void validate() const {
    if (max_to_expand == 0) {
      throw cta::exception::UserError("repack_expand.max_to_expand must be positive");
    }
  }
};

struct RepackReportRoutineConfig final {
  bool enabled = true;
  unsigned int soft_timeout_secs = 900;

  static constexpr std::size_t memberCount() { return 2; }

  void validate() const {
    if (soft_timeout_secs == 0) {
      throw cta::exception::UserError("repack_report.soft_timeout_secs must be positive");
    }
  }
};

#ifndef CTA_PGSCHED

struct QueueCleanupRoutineConfig final {
  bool enabled = true;
  unsigned int batch_size = 500;

  static constexpr std::size_t memberCount() { return 2; }

  void validate() const {
    if (batch_size == 0) {
      throw cta::exception::UserError("queue_cleanup.batch_size must be positive");
    }
  }
};

struct GarbageCollectRoutineConfig final {
  bool enabled = true;

  static constexpr std::size_t memberCount() { return 1; }

  void validate() const {}
};

#else

struct ActivePendingQueueCleanupRoutineConfig final {
  bool enabled = true;
  unsigned int batch_size = 500;
  unsigned int age_for_collection_secs = 900;

  static constexpr std::size_t memberCount() { return 3; }

  void validate() const {
    if (batch_size == 0) {
      throw cta::exception::UserError("Active/pending queue cleanup batch_size must be positive");
    }
    if (age_for_collection_secs == 0) {
      throw cta::exception::UserError("Active/pending queue cleanup age_for_collection_secs must be positive");
    }
  }
};

struct SchedulerMaintenanceCleanupRoutineConfig final {
  bool enabled = true;
  unsigned int batch_size = 500;
  unsigned int age_for_deletion_secs = 1209600;

  static constexpr std::size_t memberCount() { return 3; }

  void validate() const {
    if (batch_size == 0) {
      throw cta::exception::UserError("Scheduler maintenance cleanup batch_size must be positive");
    }
    if (age_for_deletion_secs == 0) {
      throw cta::exception::UserError("Scheduler maintenance cleanup age_for_deletion_secs must be positive");
    }
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

  void validate() const {
    if (cycle_sleep_interval_secs == 0) {
      throw cta::exception::UserError("routines.cycle_sleep_interval_secs must be positive");
    }
    if (max_cycle_duration_secs == 0) {
      throw cta::exception::UserError("routines.max_cycle_duration_secs must be positive");
    }
    disk_report_archive.validate();
    disk_report_retrieve.validate();
    repack_expand.validate();
    repack_report.validate();
#ifndef CTA_PGSCHED
    garbage_collect.validate();
    queue_cleanup.validate();
#else
    user_active_queue_cleanup.validate();
    repack_active_queue_cleanup.validate();
    user_pending_queue_cleanup.validate();
    repack_pending_queue_cleanup.validate();
    scheduler_maintenance_cleanup.validate();
#endif
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

  void validate() const {
    catalogue.validate();
    scheduler.validate();
    logging.validate();
    telemetry.validate();
    health_server.validate();
    experimental.validate();
    xrootd.validate();
    routines.validate();
  }
};

}  // namespace cta::maintd
