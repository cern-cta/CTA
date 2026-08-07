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

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (name.empty()) {
      result.addError("name", "cannot be empty");
    }
    if (device.empty()) {
      result.addError("device", "cannot be empty");
    }
    if (control_path.empty()) {
      result.addError("control_path", "cannot be empty");
    }
    if (logical_library_name.empty()) {
      result.addError("logical_library_name", "cannot be empty");
    }
    if (ready_timeout_secs == 0) {
      result.addError("ready_timeout_secs", "must be greater than zero");
    }
    return result;
  }
};

struct RmcdConfig final {
  std::string host = "localhost";
  uint16_t port = 5014;
  uint32_t request_timeout_secs = 600;
  uint32_t request_attempts = 10;

  static constexpr std::size_t memberCount() { return 4; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (host.empty()) {
      result.addError("host", "cannot be empty");
    }
    if (port == 0) {
      result.addError("port", "must be greater than zero");
    }
    if (request_timeout_secs == 0) {
      result.addError("request_timeout_secs", "must be greater than zero");
    }
    if (request_attempts == 0) {
      result.addError("request_attempts", "must be greater than zero");
    }
    return result;
  }
};

struct ArchiveUnderfillConfig final {
  uint64_t watch_period_secs = 300;
  uint64_t minimum_samples = 3;
  uint64_t start_threshold_percent = 40;
  uint64_t recovery_threshold_percent = 60;

  static constexpr std::size_t memberCount() { return 4; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (watch_period_secs == 0) {
      result.addError("watch_period_secs", "must be greater than zero");
    }
    if (minimum_samples == 0) {
      result.addError("minimum_samples", "must be greater than zero");
    }
    if (start_threshold_percent > 100) {
      result.addError("start_threshold_percent", "must not exceed 100");
    }
    if (recovery_threshold_percent > 100) {
      result.addError("recovery_threshold_percent", "must not exceed 100");
    }
    if (recovery_threshold_percent <= start_threshold_percent) {
      result.addError("recovery_threshold_percent", "must be greater than start_threshold_percent");
    }
    return result;
  }
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

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (minimum_queued_bytes == 0) {
      result.addError("minimum_queued_bytes", "must be greater than zero");
    }
    if (minimum_queued_files == 0) {
      result.addError("minimum_queued_files", "must be greater than zero");
    }
    if (scheduling_timeout_secs == 0) {
      result.addError("scheduling_timeout_secs", "must be greater than zero");
    }
    if (get_next_mount_timeout_secs == 0) {
      result.addError("get_next_mount_timeout_secs", "must be greater than zero");
    }
    if (idle_scheduling_interval_secs == 0) {
      result.addError("idle_scheduling_interval_secs", "must be greater than zero");
    }
    if (drive_state_poll_interval_secs == 0) {
      result.addError("drive_state_poll_interval_secs", "must be greater than zero");
    }
    if (mount_timeout_secs == 0) {
      result.addError("mount_timeout_secs", "must be greater than zero");
    }
    if (tape_load_timeout_secs == 0) {
      result.addError("tape_load_timeout_secs", "must be greater than zero");
    }
    if (unmount_timeout_secs == 0) {
      result.addError("unmount_timeout_secs", "must be greater than zero");
    }
    return result;
  }
};

struct ArchiveTransferConfig final {
  uint64_t fetch_max_bytes = 100000000000;
  uint64_t fetch_max_files = 5000;
  uint64_t flush_max_bytes = 32000000000;
  uint64_t flush_max_files = 200;
  ArchiveUnderfillConfig underfill;

  static constexpr std::size_t memberCount() { return 5; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (fetch_max_bytes == 0) {
      result.addError("fetch_max_bytes", "must be greater than zero");
    }
    if (fetch_max_files == 0) {
      result.addError("fetch_max_files", "must be greater than zero");
    }
    if (flush_max_bytes == 0) {
      result.addError("flush_max_bytes", "must be greater than zero");
    }
    if (flush_max_files == 0) {
      result.addError("flush_max_files", "must be greater than zero");
    }
    result.merge("underfill", underfill.validate());
    return result;
  }
};

struct EncryptionConfig final {
  bool enabled = true;
  std::string external_key_script = "/usr/local/bin/cta-get-encryption-key.sh";

  static constexpr std::size_t memberCount() { return 2; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (enabled && external_key_script.empty()) {
      result.addError("external_key_script", "cannot be empty when encryption is enabled");
    }
    return result;
  }
};

struct RaoConfig final {
  bool enabled = true;
  std::string lto_algorithm = "sltf";

  static constexpr std::size_t memberCount() { return 2; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (lto_algorithm != "linear" && lto_algorithm != "random" && lto_algorithm != "sltf") {
      result.addError("lto_algorithm",
                      "has unsupported value '" + lto_algorithm + "'; must be one of [linear, random, sltf]");
    }
    return result;
  }
};

struct RetrieveTransferConfig final {
  uint64_t fetch_max_bytes = 100000000000;
  uint64_t fetch_max_files = 5000;
  uint32_t drain_to_disk_timeout_secs = 1800;
  std::string external_free_disk_space_script = "";
  RaoConfig rao;

  static constexpr std::size_t memberCount() { return 5; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (fetch_max_bytes == 0) {
      result.addError("fetch_max_bytes", "must be greater than zero");
    }
    if (fetch_max_files == 0) {
      result.addError("fetch_max_files", "must be greater than zero");
    }
    if (drain_to_disk_timeout_secs == 0) {
      result.addError("drain_to_disk_timeout_secs", "must be greater than zero");
    }
    result.merge("rao", rao.validate());
    return result;
  }
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

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (buffer_count == 0) {
      result.addError("buffer_count", "must be greater than zero");
    }
    if (buffer_size_bytes == 0) {
      result.addError("buffer_size_bytes", "must be greater than zero");
    }
    if (disk_io_threads == 0) {
      result.addError("disk_io_threads", "must be greater than zero");
    }
    if (no_block_move_timeout_secs == 0) {
      result.addError("no_block_move_timeout_secs", "must be greater than zero");
    }
    result.merge("archive", archive.validate());
    result.merge("retrieve", retrieve.validate());
    result.merge("encryption", encryption.validate());
    return result;
  }
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

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    result.merge("catalogue", catalogue.validate());
    result.merge("scheduler", scheduler.validate());
    result.merge("logging", logging.validate());
    result.merge("telemetry", telemetry.validate());
    result.merge("health_server", health_server.validate());
    result.merge("experimental", experimental.validate());
    result.merge("xrootd", xrootd.validate());
    result.merge("drive", drive.validate());
    result.merge("mounts", mounts.validate());
    result.merge("rmcd", rmcd.validate());
    result.merge("transfers", transfers.validate());
    return result;
  }
};

}  // namespace cta::tape::daemon
