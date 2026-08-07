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
  uint32_t ready_timeout_secs = 120;

  static constexpr std::size_t memberCount() { return 5; }
};

struct RmcdConfig final {
  std::string host = "localhost";
  uint16_t port = 5014;
  uint32_t request_timeout_secs = 600;
  uint32_t request_attempts = 10;

  static constexpr std::size_t memberCount() { return 4; }
};

struct ArchiveUnderfillConfig final {
  uint64_t watch_period_secs = 300;
  uint64_t minimum_samples = 3;
  uint64_t start_threshold_percent = 40;
  uint64_t recovery_threshold_percent = 60;

  static constexpr std::size_t memberCount() { return 4; }
};

struct MountsConfig final {
  uint64_t minimum_queued_bytes = 500000000000;
  uint64_t minimum_queued_files = 10000;
  uint32_t scheduling_timeout_secs = 300;
  uint32_t get_next_mount_timeout_secs = 900;
  uint32_t idle_scheduling_interval_secs = 10;
  uint32_t drive_state_poll_interval_secs = 5;
  uint32_t mount_timeout_secs = 600;
  uint32_t tape_load_timeout_secs = 300;
  uint32_t unmount_timeout_secs = 900;

  static constexpr std::size_t memberCount() { return 9; }
};

struct ArchiveTransferConfig final {
  uint64_t fetch_max_bytes = 100000000000;
  uint64_t fetch_max_files = 5000;
  uint64_t flush_max_bytes = 32000000000;
  uint64_t flush_max_files = 200;
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
  uint64_t fetch_max_bytes = 100000000000;
  uint64_t fetch_max_files = 5000;
  uint32_t drain_to_disk_timeout_secs = 1800;
  std::string external_free_disk_space_script = "/usr/bin/cta-eosdf.sh";
  RaoConfig rao;

  static constexpr std::size_t memberCount() { return 5; }
};

struct TransfersConfig final {
  uint32_t buffer_count = 5000;
  uint32_t buffer_size_bytes = 5000000;
  uint32_t disk_io_threads = 10;
  uint32_t no_block_move_timeout_secs = 1800;
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
