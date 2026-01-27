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
  bool telemetry = false;
};

struct CatalogueConfig {
  std::string config_file = "/etc/cta/cta-catalogue.conf";
};

struct SchedulerConfig {
  std::string objectstore_backendpath;
  std::int64_t tape_cache_max_age_secs = 600;
  std::int64_t retrieve_queue_cache_max_age_secs = 10;
};

struct KeyValue {
  std::string key;
  std::string value;
};

struct LoggingConfig {
  std::string level = "INFO";
  std::map<std::string, std::string> attributes;
};

struct BasicAuthConfig {
  std::string username;
  std::string password_file;
};

struct TelemetryMetricsOtlpConfig {
  std::string endpoint;
  BasicAuthConfig auth;
};

struct TelemetryMetricsFileConfig {
  std::string path;
};

struct TelemetryMetricsConfig {
  cta::telemetry::MetricsBackend backend;
  std::int64_t interval_ms = 15000;
  std::int64_t timeout_ms = 3000;
  // TODO: ensure we enforce the correct checks here
  // TODO: for now just enforce both to be here
  // Hopefully we can get rid of this using the declarative config
  TelemetryMetricsOtlpConfig otlp;
  TelemetryMetricsFileConfig file;
};

struct TelemetryConfig {
  bool retain_instance_id_on_restart = false;
  TelemetryMetricsConfig metrics;
};

/// -- end

struct GarbageCollectRoutine {
  bool enabled = true;
};

struct DiskReportRoutine {
  bool enabled = true;
  std::int64_t batch_size = 500;
  std::int64_t soft_timeout_secs = 30;
};

struct QueueCleanupRoutine {
  bool enabled = true;
  std::int64_t batch_size = 500;
};

struct RepackExpandRoutine {
  bool enabled = true;
  std::int64_t max_to_expand = 900;
};

struct RepackReportRoutine {
  bool enabled = true;
  std::int64_t soft_timeout_secs = 900;
};

struct ActivePendingQueueCleanupRoutine {
  bool enabled = true;
  std::int64_t batch_size = 500;
  std::int64_t age_for_collection_secs = 900;
};

struct SchedulerMaintenanceCleanupRoutine {
  bool enabled = true;
  std::int64_t batch_size = 500;
  std::int64_t age_for_deletion_secs = 1209600;
};

struct RoutinesConfig {
  std::int64_t global_sleep_interval_secs = 1;

  DiskReportRoutine disk_report_archive;
  DiskReportRoutine disk_report_retrieve;

  GarbageCollectRoutine garbage_collect;

  QueueCleanupRoutine queue_cleanup;

  RepackExpandRoutine repack_expand;
  RepackReportRoutine repack_report;

  ActivePendingQueueCleanupRoutine user_active_queue_cleanup;
  ActivePendingQueueCleanupRoutine repack_active_queue_cleanup;
  ActivePendingQueueCleanupRoutine user_pending_queue_cleanup;
  ActivePendingQueueCleanupRoutine repack_pending_queue_cleanup;

  SchedulerMaintenanceCleanupRoutine scheduler_maintenance_cleanup;
};

struct MaintdConfig {
  ExperimentalConfig experimental;
  CatalogueConfig catalogue;
  SchedulerConfig scheduler;
  LoggingConfig logging;
  RoutinesConfig routines;
  TelemetryConfig telemetry;
};

}  // namespace cta::maintd
