/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/telemetry/config/TelemetryConfig.hpp"

#include <cstdint>
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
  std::string objectstore_backend_path;
  int tape_cache_max_age_secs = 600;
  int retrieve_queue_cache_max_age_secs = 10;
};

struct LoggingConfig {
  std::string level = "INFO";
  std::map<std::string, std::string> attributes;
};

struct TelemetryConfig {
  std::string config;
};

struct HealthServerConfig {
  bool enabled;
  std::string host;
  int port;
};

/// -- end

struct GarbageCollectRoutine {
  bool enabled = true;
};

struct DiskReportRoutine {
  bool enabled = true;
  int batch_size = 500;
  int soft_timeout_secs = 30;
};

struct QueueCleanupRoutine {
  bool enabled = true;
  int batch_size = 500;
};

struct RepackExpandRoutine {
  bool enabled = true;
  int max_to_expand = 900;
};

struct RepackReportRoutine {
  bool enabled = true;
  int soft_timeout_secs = 900;
};

struct ActivePendingQueueCleanupRoutine {
  bool enabled = true;
  int batch_size = 500;
  int age_for_collection_secs = 900;
};

struct SchedulerMaintenanceCleanupRoutine {
  bool enabled = true;
  int batch_size = 500;
  int age_for_deletion_secs = 1209600;
};

struct RoutinesConfig {
  int global_sleep_interval_secs = 1;
  int liveness_window = 120;

  DiskReportRoutine disk_report_archive;
  DiskReportRoutine disk_report_retrieve;

  RepackExpandRoutine repack_expand;
  RepackReportRoutine repack_report;

  GarbageCollectRoutine garbage_collect;

  QueueCleanupRoutine queue_cleanup;

  // ActivePendingQueueCleanupRoutine user_active_queue_cleanup;
  // ActivePendingQueueCleanupRoutine repack_active_queue_cleanup;
  // ActivePendingQueueCleanupRoutine user_pending_queue_cleanup;
  // ActivePendingQueueCleanupRoutine repack_pending_queue_cleanup;

  // SchedulerMaintenanceCleanupRoutine scheduler_maintenance_cleanup;
};

struct MaintdConfig {
  CatalogueConfig catalogue;
  SchedulerConfig scheduler;
  LoggingConfig logging;
  TelemetryConfig telemetry;
  HealthServerConfig health_server;
  RoutinesConfig routines;
  ExperimentalConfig experimental;
};

}  // namespace cta::maintd
