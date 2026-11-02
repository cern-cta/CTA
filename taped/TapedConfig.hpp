/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "runtime/config/CommonConfig.hpp"

#include <cstdint>
#include <string>

namespace cta::tape::daemon {

/// Opt-in automatic activation on first registration in the catalogue.
struct DriveRegistrationConfig final {
  /// Automatically request desired Up on first registration; otherwise register desired Down.
  bool desired_up = false;

  static constexpr std::size_t memberCount() { return 1; }

  /// Both registration policies are valid.
  cta::runtime::ValidationResult validate() const { return {}; }
};

/// Opt-in automatic startup of drives already present in the catalogue.
struct DriveStartupConfig final {
  /// Preserve an existing desired Up state at startup; recovering a loaded drive also requires eject_on_up.
  bool preserve_desired_up = false;
  /// Automatically request Up for eligible existing Down drives; operator and failure Down reasons prevent activation.
  bool auto_up = false;

  static constexpr std::size_t memberCount() { return 2; }

  /// Accept all combinations of startup flags.
  cta::runtime::ValidationResult validate() const { return {}; }
};

/// Preparation policy shared by registration, startup and operator-requested Up transitions.
struct DriveLifecycleConfig final {
  /// Eject any cartridge on transition from Down to Up; otherwise a loaded drive returns to Down.
  bool eject_on_up = false;
  /// Automatic startup policy for drives already registered in the catalogue.
  DriveStartupConfig startup;
  /// Desired-state policy for first registration in the catalogue.
  DriveRegistrationConfig registration;

  static constexpr std::size_t memberCount() { return 3; }

  /// Collect validation errors from startup and registration policies.
  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    result.merge("startup", startup.validate());
    result.merge("registration", registration.validate());
    return result;
  }
};

/// Drive identity, device paths and preparation policy.
struct DriveConfig final {
  /// Unique CTA drive name used in the catalogue and log records.
  std::string name = "";
  /// Path to the non-rewinding tape device, preferably a persistent device symlink.
  std::string device = "";
  /// SCSI media-changer address: "smc" followed by the drive ordinal reported by cta-smc -q D.
  std::string control_path = "";
  /// Name of the existing catalogue logical library containing this drive.
  std::string logical_library_name = "";

  /// Preparation and desired-state policies for drive lifecycle transitions.
  DriveLifecycleConfig lifecycle;

  static constexpr std::size_t memberCount() { return 5; }

  /// Collect missing drive identity and path errors, including startup and registration validation.
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
    result.merge("lifecycle", lifecycle.validate());
    return result;
  }
};

/// Media-changer connection and retry settings.
struct RmcdConfig final {
  /// Hostname or IP address of the remote media-changer daemon.
  std::string host = "localhost";
  /// TCP port used for RMCD requests.
  uint16_t port = 5014;
  /// Timeout in seconds for network reads and writes to RMCD; expiry fails the current operation.
  uint32_t request_timeout_secs = 600;
  /// Maximum attempts for retriable RMC requests; network errors, protocol errors and timeouts are not retried.
  uint32_t request_attempts = 10;

  static constexpr std::size_t memberCount() { return 4; }

  /// Collect invalid media-changer endpoint and retry settings.
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

/// Thresholds for detecting and recovering from archive underfill.
struct ArchiveUnderfillConfig final {
  /// Minimum underfill observation duration in seconds before requesting unmount; minimum_samples must also be met.
  uint64_t watch_period_secs = 300;
  /// Minimum underfilled batch count before requesting unmount; watch_period_secs must also elapse.
  uint64_t minimum_samples = 3;
  /// Fill percentage below which observation starts, using the larger of the byte and file batch-fill ratios.
  uint64_t start_threshold_percent = 40;
  /// Fill percentage at or above which observation resets; must exceed start_threshold_percent.
  uint64_t recovery_threshold_percent = 60;

  static constexpr std::size_t memberCount() { return 4; }

  /// Collect invalid sampling settings and inconsistent underfill thresholds.
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

/// Scheduling intervals and drive-phase timeouts.
struct MountsConfig final {
  /// Queued-byte threshold for a mount, ORed with minimum_queued_files.
  /// Catalogue mount-rule timeouts can allow mounts below these thresholds.
  uint64_t minimum_queued_bytes = 500000000000;
  /// Queued-file threshold for a mount, ORed with minimum_queued_bytes.
  uint64_t minimum_queued_files = 10000;
  /// Mount-selection timeout in seconds; expiry logs a warning and retries the request.
  uint32_t get_next_mount_timeout_secs = 900;
  /// Retry delay in seconds when scheduling finds no work or fails, or after a failed session on a reusable drive.
  uint32_t idle_scheduling_interval_secs = 10;
  /// Interval in seconds for polling the desired drive state while the drive is Down.
  uint32_t drive_state_poll_interval_secs = 5;
  /// Retry interval in seconds while waiting for a missing logical library to appear.
  uint32_t logical_library_poll_interval_secs = 5;
  /// Maximum seconds in Mounting before liveness becomes unhealthy.
  uint32_t mount_timeout_secs = 900;
  /// Maximum seconds in Loading before liveness becomes unhealthy.
  /// Also bounds the physical drive readiness wait, whose failure starts cleanup.
  uint32_t tape_load_timeout_secs = 900;
  /// Maximum seconds in Unloading before liveness becomes unhealthy, including rewind, label reading and unload.
  uint32_t tape_unload_timeout_secs = 900;
  /// Maximum seconds in Unmounting before liveness becomes unhealthy.
  uint32_t unmount_timeout_secs = 900;
  /// Maximum seconds in Preparing before liveness becomes unhealthy.
  uint32_t preparing_timeout_secs = 900;
  /// Maximum seconds in Finalizing before liveness becomes unhealthy.
  uint32_t finalizing_timeout_secs = 900;

  static constexpr std::size_t memberCount() { return 12; }

  /// Collect zero scheduling thresholds, polling intervals and phase timeouts.
  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (minimum_queued_bytes == 0) {
      result.addError("minimum_queued_bytes", "must be greater than zero");
    }
    if (minimum_queued_files == 0) {
      result.addError("minimum_queued_files", "must be greater than zero");
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
    if (logical_library_poll_interval_secs == 0) {
      result.addError("logical_library_poll_interval_secs", "must be greater than zero");
    }
    if (mount_timeout_secs == 0) {
      result.addError("mount_timeout_secs", "must be greater than zero");
    }
    if (tape_load_timeout_secs == 0) {
      result.addError("tape_load_timeout_secs", "must be greater than zero");
    }
    if (tape_unload_timeout_secs == 0) {
      result.addError("tape_unload_timeout_secs", "must be greater than zero");
    }
    if (preparing_timeout_secs == 0) {
      result.addError("preparing_timeout_secs", "must be greater than zero");
    }
    if (finalizing_timeout_secs == 0) {
      result.addError("finalizing_timeout_secs", "must be greater than zero");
    }
    if (unmount_timeout_secs == 0) {
      result.addError("unmount_timeout_secs", "must be greater than zero");
    }
    return result;
  }
};

/// Archive batching, flushing and underfill settings.
struct ArchiveTransferConfig final {
  /// Maximum bytes requested per scheduler archive batch; either this or fetch_max_files limits the batch.
  uint64_t fetch_max_bytes = 100000000000;
  /// Maximum files requested per scheduler archive batch; either this or fetch_max_bytes limits the batch.
  uint64_t fetch_max_files = 5000;
  /// Byte threshold for flushing with a synchronized tape mark.
  /// Flushes occur at file boundaries, so the actual byte count can exceed this threshold.
  uint64_t flush_max_bytes = 32000000000;
  /// File threshold for tape flushes; reaching this or flush_max_bytes triggers a flush.
  uint64_t flush_max_files = 200;
  /// Observation window and hysteresis for ending persistently underfilled archive sessions.
  ArchiveUnderfillConfig underfill;

  static constexpr std::size_t memberCount() { return 5; }

  /// Collect invalid archive batch limits and underfill settings.
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

/// Tape encryption and external key-script settings.
struct EncryptionConfig final {
  /// Use drive hardware encryption when a valid key is configured for the tape or tape pool.
  bool enabled = true;
  /// Executable that obtains keys using the protocol expected by EncryptionControl.
  std::string external_key_script = "/usr/local/bin/cta-get-encryption-key.sh";

  static constexpr std::size_t memberCount() { return 2; }

  /// Require a key-script path when encryption is enabled.
  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (enabled && external_key_script.empty()) {
      result.addError("external_key_script", "cannot be empty when encryption is enabled");
    }
    return result;
  }
};

/// Recommended access order settings for retrieval.
struct RaoConfig final {
  /// Enable recommended access order on supported drives, preferring hardware RAO over software RAO.
  bool enabled = true;
  /// Software RAO algorithm for LTO-8: linear, random or sltf; ignored when hardware RAO is available.
  /// SLTF requires catalogue media-type fields NB_WRAPS, MIN_LPOS and MAX_LPOS.
  std::string lto_algorithm = "sltf";

  static constexpr std::size_t memberCount() { return 2; }

  /// Reject unknown LTO access-order algorithms, even when RAO is disabled.
  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (lto_algorithm != "linear" && lto_algorithm != "random" && lto_algorithm != "sltf") {
      result.addError("lto_algorithm",
                      "has unsupported value '" + lto_algorithm + "'; must be one of [linear, random, sltf]");
    }
    return result;
  }
};

/// Retrieval batching, disk delivery and access-order settings.
struct RetrieveTransferConfig final {
  /// Maximum bytes requested per scheduler retrieve batch; either this or fetch_max_files limits the batch.
  uint64_t fetch_max_bytes = 100000000000;
  /// Maximum files requested per scheduler retrieve batch; either this or fetch_max_bytes limits the batch.
  uint64_t fetch_max_files = 5000;
  /// Maximum seconds in DrainingToDisk before liveness becomes unhealthy.
  uint32_t drain_to_disk_timeout_secs = 1800;
  /// Executable reporting free space at the retrieve destination; an empty path disables disk-space backpressure.
  std::string external_free_disk_space_script = "";
  /// Recommended access order policy for reducing tape positioning during retrieval.
  RaoConfig rao;

  static constexpr std::size_t memberCount() { return 5; }

  /// Collect invalid retrieval batch limits, disk-drain timeout and RAO settings.
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

/// Shared pipeline resources and direction-specific transfer settings.
struct TransfersConfig final {
  /// Number of pipeline buffers; approximate buffer memory is buffer_count multiplied by buffer_size_bytes.
  uint32_t buffer_count = 5000;
  /// Size in bytes of each in-memory transfer buffer.
  uint32_t buffer_size_bytes = 5000000;
  /// Number of disk I/O workers, limiting concurrent file transfers performing disk I/O.
  uint32_t disk_io_threads = 10;
  /// Interval in seconds between tape-session statistics reports; final statistics are also reported at session end.
  uint32_t stats_report_interval_secs = 15;
  /// Maximum seconds without block movement in Transferring before liveness becomes unhealthy.
  /// Measured from state entry or the last block movement, whichever is later; also sets stuck-file warning intervals.
  uint32_t no_block_move_timeout_secs = 1800;
  /// Batching, tape flush and underfill policy for disk-to-tape transfers.
  ArchiveTransferConfig archive;
  /// Batching, disk delivery and access order policy for tape-to-disk transfers.
  RetrieveTransferConfig retrieve;
  /// Hardware encryption and key retrieval settings shared by archive and retrieve sessions.
  EncryptionConfig encryption;

  static constexpr std::size_t memberCount() { return 8; }

  /// Collect invalid pipeline resource limits, reporting intervals and transfer settings.
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
    if (stats_report_interval_secs == 0) {
      result.addError("stats_report_interval_secs", "must be greater than zero");
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

/// Configuration consumed by the taped runtime and daemon.
struct TapedConfig final {
  /// Catalogue database connection configuration.
  cta::runtime::CatalogueConfig catalogue;
  /// Scheduler backend connection and queue-statistics cache settings.
  cta::runtime::SchedulerConfig scheduler;
  /// Log severity, format and monitoring attributes.
  cta::runtime::LoggingConfig logging;
  /// OpenTelemetry initialization settings; also requires experimental.telemetry_enabled.
  cta::runtime::TelemetryConfig telemetry;
  /// Readiness and liveness endpoint settings.
  cta::runtime::HealthServerConfig health_server;
  /// Opt-in feature gates for experimental functionality.
  cta::runtime::ExperimentalConfig experimental;
  /// XRootD authentication settings for connections to disk storage.
  cta::runtime::XRootDConfig xrootd;

  /// Drive identity, device paths and registration/startup policy.
  DriveConfig drive;
  /// Mount scheduling thresholds, polling intervals and phase timeouts.
  MountsConfig mounts;
  /// RMCD connection settings for physical mount and unmount operations.
  RmcdConfig rmcd;
  /// Shared pipeline resources and archive/retrieve transfer policies.
  TransfersConfig transfers;

  static constexpr std::size_t memberCount() { return 11; }

  /// Collect validation errors from all sections, prefixed with their section names.
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
