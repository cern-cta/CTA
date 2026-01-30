/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>

namespace cta::maintd {

// This should go into common config

struct ExperimentalConfig {
  bool telemetry_enabled = false;
};

struct CatalogueConfig {
  std::string config_file = "/etc/cta/cta-catalogue.conf";
};

struct SchedulerConfig {
  std::string backend_name;
  std::string objectstore_backend_path;
  int tape_cache_max_age_secs = 600;
  int retrieve_queue_cache_max_age_secs = 10;
};

struct LoggingConfig {
  std::string level = "INFO";
  std::map<std::string, std::string> attributes;
};

struct TelemetryConfig {
  std::string config = "";
};

struct HealthServerConfig {
  bool enabled;
  std::string host;
  int port;
};

struct XRootDConfig {
  std::string security_protocol = "sss";
  std::string sss_keytab_path;
};

/// -- end

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
  CatalogueConfig catalogue;
  SchedulerConfig scheduler;
  LoggingConfig logging;
  TelemetryConfig telemetry;
  HealthServerConfig health_server;
  RoutinesConfig routines;
  ExperimentalConfig experimental;
  XRootDConfig xrootd;
};

}  // namespace cta::maintd
