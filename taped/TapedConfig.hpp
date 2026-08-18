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
  int ready_timeout_secs = 120;

  static constexpr std::size_t memberCount() { return 5; }
};

struct RmcdConfig final {
  std::string host = "localhost";
  int port = 5014;
  int request_timeout_secs = 600;
  int request_attempts = 10;

  static constexpr std::size_t memberCount() { return 4; }
};

struct ArchiveUnderfillConfig final {
  int watch_period_secs = 300;
  int minimum_samples = 3;
  int start_threshold_percent = 40;
  int recovery_threshold_percent = 60;

  static constexpr std::size_t memberCount() { return 4; }
};

struct MountsConfig final {
  long minimum_queued_bytes = 500000000000;
  long minimum_queued_files = 10000;
  int scheduling_timeout_secs = 300;
  int get_next_mount_timeout_secs = 900;
  int idle_scheduling_interval_secs = 10;
  int mount_timeout_secs = 600;
  int tape_load_timeout_secs = 300;
  int unmount_timeout_secs = 900;

  static constexpr std::size_t memberCount() { return 8; }
};

struct ArchiveTransferConfig final {
  long fetch_max_bytes = 100000000000;
  long fetch_max_files = 5000;
  long flush_max_bytes = 32000000000;
  long flush_max_files = 200;
  ArchiveUnderfillConfig underfill;

  static constexpr std::size_t memberCount() { return 5; }
};

struct EncryptionConfig final {
  bool enabled = true;
  std::string external_key_script = "/usr/local/bin/cta-get-encryption-key.sh";

  static constexpr std::size_t memberCount() { return 2; }
};

struct RaoConfig final {
  bool enabled = true;
  std::string lto_algorithm = "sltf";

  static constexpr std::size_t memberCount() { return 2; }
};

struct RetrieveTransferConfig final {
  long fetch_max_bytes = 100000000000;
  long fetch_max_files = 5000;
  int drain_to_disk_timeout_secs = 1800;
  std::string external_free_disk_space_script = "/usr/bin/cta-eosdf.sh";
  RaoConfig rao;

  static constexpr std::size_t memberCount() { return 5; }
};

struct TransfersConfig final {
  int buffer_count = 5000;
  int buffer_size_bytes = 5000000;
  int disk_io_threads = 10;
  int no_block_move_timeout_secs = 1800;
  ArchiveTransferConfig archive;
  RetrieveTransferConfig retrieve;
  EncryptionConfig encryption;

  static constexpr std::size_t memberCount() { return 7; }
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
  MountsConfig mounts;
  RmcdConfig rmcd;
  TransfersConfig transfers;

  static constexpr std::size_t memberCount() { return 11; }
};

}  // namespace cta::tape::daemon
