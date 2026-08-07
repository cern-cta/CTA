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

namespace cta::tape::daemon {

struct DriveConfig final {
  std::string name = "";
  std::string device = "";
  std::string control_path = "";
  std::string logical_library_name = "";

  static constexpr std::size_t memberCount() { return 4; }
};

struct MemoryConfig final {
  int buffer_count = 5000;
  int buffer_size_bytes = 5000000;

  static constexpr std::size_t memberCount() { return 2; }
};

struct ArchiveDismountPolicyConfig final {
  int underfill_watch_period_secs = 300;
  int underfill_min_samples = 3;
  int underfill_start_threshold = 40;
  int underfill_recovery_threshold = 60;

  static constexpr std::size_t memberCount() { return 4; }
};

struct SchedulingConfig final {
  // TODO: add min/max?
  long mount_criteria_bytes = 500000000000;
  long mount_criteria_files = 10000;

  long archive_fetch_bytes = 80000000000;
  long archive_fetch_files = 4000;

  long archive_flush_bytes = 32000000000;
  long archive_flush_files = 200;

  long retrieve_fetch_bytes = 80000000000;
  long retrieve_fetch_files = 4000;

  ArchiveDismountPolicyConfig archive_dismount_policy;

  static constexpr std::size_t memberCount() { return 9; }
};

struct EncryptionConfig final {
  bool enabled = true;
  std::string external_encryption_key_script = "/usr/local/bin/cta-get-encryption-key.sh";

  static constexpr std::size_t memberCount() { return 2; }
};

struct DiskConfig final {
  int io_threads = 10;
  std::string external_free_disk_space_script = "/usr/bin/cta-eosdf.sh";

  static constexpr std::size_t memberCount() { return 2; }
};

struct RaoConfig final {
  // TODO: hardware RAO?
  bool enabled = true;
  std::string lto_algorithm = "sltf";
  std::string lto_algorithm_options = "cost_heuristic_name:cta";

  static constexpr std::size_t memberCount() { return 3; }
};

// Don't like this layout
struct TimeoutConfig final {
  int tape_load_timeout_secs = 300;
  int drive_ready_timeout_secs = 120;
  int schedule_timeout_secs = 300;
  int mount_timeout_secs = 600;
  int unmount_timeout_secs = 600;
  int drain_to_disk_timeout_secs = 1800;
  int shutdown_timeout_secs = 900;
  int no_block_move_timeout_secs = 1800;
  int idle_scheduling_interval_secs = 10;
  int get_next_mount_timeout_secs = 900;

  static constexpr std::size_t memberCount() { return 10; }
};

struct RmcConfig final {
  std::string host = "localhost";
  int port = 5014;
  int timeout_secs = 600;
  int request_attempts = 10;

  static constexpr std::size_t memberCount() { return 4; }
};

struct TapedConfig final {
  cta::runtime::CatalogueConfig catalogue;
  cta::runtime::SchedulerConfig scheduler;
  cta::runtime::LoggingConfig logging;
  cta::runtime::TelemetryConfig telemetry;
  cta::runtime::HealthServerConfig health_server;
  cta::runtime::ExperimentalConfig experimental;
  cta::runtime::XRootDConfig xrootd;

  DriveConfig drive;
  MemoryConfig memory;
  SchedulingConfig scheduling;
  EncryptionConfig encryption;
  DiskConfig disk;
  RaoConfig rao;
  TimeoutConfig timeout;
  RmcConfig rmc;

  static constexpr std::size_t memberCount() { return 15; }
};

}  // namespace cta::tape::daemon
