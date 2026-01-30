/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/runtime/CommonConfig.hpp"

#include <cstdint>
#include <map>
#include <optional>
#include <string>

namespace cta::maintd {

struct DiskReportRoutineConfig {
  bool enabled = true;
  int batch_size = 500;
  int soft_timeout_secs = 30;
};

struct RepackExpandRoutineConfig {
  bool enabled = true;
  int max_to_toexpand = 900;
};

struct RepackReportRoutineConfig {
  bool enabled = true;
  int soft_timeout_secs = 900;
};

#ifndef CTA_PGSCHED
struct QueueCleanupRoutineConfig {
  bool enabled = true;
  int batch_size = 500;
};

struct GarbageCollectRoutineConfig {
  bool enabled = true;
};
#else
struct ActivePendingQueueCleanupRoutineConfig {
  bool enabled = true;
  int batch_size = 500;
  int age_for_collection_secs = 900;
};

struct SchedulerMaintenanceCleanupRoutineConfig {
  bool enabled = true;
  int batch_size = 500;
  int age_for_deletion_secs = 1209600;
};
#endif

struct RoutinesConfig {
  int global_sleep_interval_secs = 10;
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
};

struct MaintdConfig {
  cta::runtime::CatalogueConfig catalogue;
  cta::runtime::SchedulerConfig scheduler;
  cta::runtime::LoggingConfig logging;
  cta::runtime::TelemetryConfig telemetry;
  cta::runtime::HealthServerConfig health_server;
  cta::runtime::ExperimentalConfig experimental;
  cta::runtime::XRootDConfig xrootd;
  RoutinesConfig routines;
};

}  // namespace cta::maintd
