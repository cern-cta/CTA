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
  int batch_size = 500;
  int soft_timeout_secs = 30;

  static constexpr std::size_t memberCount() { return 3; }
};

struct RepackExpandRoutineConfig final {
  bool enabled = true;
  int max_to_expand = 2;

  static constexpr std::size_t memberCount() { return 2; }
};

struct RepackReportRoutineConfig final {
  bool enabled = true;
  int soft_timeout_secs = 900;

  static constexpr std::size_t memberCount() { return 2; }
};

#ifndef CTA_PGSCHED

struct QueueCleanupRoutineConfig final {
  bool enabled = true;
  int batch_size = 500;

  static constexpr std::size_t memberCount() { return 2; }
};

struct GarbageCollectRoutineConfig final {
  bool enabled = true;

  static constexpr std::size_t memberCount() { return 1; }
};

#else

struct ActivePendingQueueCleanupRoutineConfig final {
  bool enabled = true;
  int batch_size = 500;
  int age_for_collection_secs = 900;

  static constexpr std::size_t memberCount() { return 3; }
};

struct SchedulerMaintenanceCleanupRoutineConfig final {
  bool enabled = true;
  int batch_size = 500;
  int age_for_deletion_secs = 1209600;

  static constexpr std::size_t memberCount() { return 3; }
};

#endif

struct RoutinesConfig final {
  int cycle_sleep_interval_secs = 10;
  int max_cycle_duration_secs = 900;

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
};

}  // namespace cta::maintd
