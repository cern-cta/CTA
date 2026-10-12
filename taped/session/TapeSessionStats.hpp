/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <cstdint>

namespace cta::tape::daemon {

/// Statistics for transferring data to or from tape.
struct TapeTransferStats {
  /// Accumulated tape positioning time during transfer, in seconds.
  double positionTime = 0;

  /// Accumulated time spent computing checksums, in seconds.
  double checksumingTime = 0;

  /// Accumulated drive read/write time for payload and headers, in seconds.
  double readWriteTime = 0;

  /// Accumulated time spent flushing tape writes, in seconds.
  double flushTime = 0;

  /// Accumulated time spent waiting for data blocks, in seconds.
  double waitDataTime = 0;

  /// Accumulated time spent waiting for free memory, in seconds.
  double waitFreeMemoryTime = 0;

  /// Accumulated time spent by the tape thread waiting for a task, in seconds.
  double waitInstructionsTime = 0;

  /// @brief Return transfer duration in seconds, including the supplied reporting wait.
  ///
  /// Excludes loading, tape positioning and cleanup.
  /// @param waitReportingTime Accumulated reporting wait time, in seconds.
  double transferTime(double waitReportingTime) const {
    return checksumingTime + readWriteTime + flushTime + waitDataTime + waitFreeMemoryTime + waitInstructionsTime
           + waitReportingTime;
  }

  /// Accumulated data volume (actual payload), in bytes.
  uint64_t dataVolume = 0;

  /// Accumulated file header and trailer volume, in bytes.
  uint64_t headerVolume = 0;

  /// Number of files actually transferred in the session, in files.
  uint64_t filesCount = 0;

  /// Number of files transferred for repack retrieve requests, in files.
  uint64_t repackFilesCount = 0;

  /// Number of files transferred for user retrieve requests, in files.
  uint64_t userFilesCount = 0;

  /// Number of files processed for verify-only retrieve requests, in files.
  uint64_t verifiedFilesCount = 0;

  /// Payload transferred for repack retrieve requests, in bytes.
  uint64_t repackBytesCount = 0;

  /// Payload transferred for user retrieve requests, in bytes.
  uint64_t userBytesCount = 0;

  /// Payload processed for verify-only retrieve requests, in bytes.
  uint64_t verifiedBytesCount = 0;

  /// File header volume: three 80-byte records, in bytes per file.
  static const uint64_t headerVolumePerFile = 3 * 80;
  /// File trailer volume: three 80-byte records, in bytes per file.
  static const uint64_t trailerVolumePerFile = 3 * 80;

  /// Accumulate all transfer counters and durations from another statistics block.
  void add(const TapeTransferStats& other) {
    positionTime += other.positionTime;
    checksumingTime += other.checksumingTime;
    readWriteTime += other.readWriteTime;
    flushTime += other.flushTime;
    waitDataTime += other.waitDataTime;
    waitFreeMemoryTime += other.waitFreeMemoryTime;
    waitInstructionsTime += other.waitInstructionsTime;
    dataVolume += other.dataVolume;
    headerVolume += other.headerVolume;
    filesCount += other.filesCount;
    repackFilesCount += other.repackFilesCount;
    userFilesCount += other.userFilesCount;
    verifiedFilesCount += other.verifiedFilesCount;
    repackBytesCount += other.repackBytesCount;
    userBytesCount += other.userBytesCount;
    verifiedBytesCount += other.verifiedBytesCount;
  }
};

/// Statistics for disk transfers.
struct DiskTransferStats {
  /// Wall-clock delivery duration in seconds; not summed across workers.
  double deliveryTime = 0;
  /// Accumulated reporting wait time across contributing workers, in seconds.
  double waitReportingTime = 0;

  /// Accumulate reporting wait time, leaving wall-clock delivery time unchanged.
  void add(const DiskTransferStats& other) {
    // deliveryTime is elapsed wall-clock time and is not cumulative between threads.
    waitReportingTime += other.waitReportingTime;
  }
};

/// Statistics for restoring the drive and returning the cartridge to the library.
/// Durations accumulate across attempts; individual operations overlap the total cleanup time.
struct TapeCleanupStats {
  /// Unload time, in seconds.
  double unloadTime = 0;

  /// Unmount time, in seconds.
  double unmountTime = 0;

  /// Total elapsed cleanup time, including preparation and recovery retries, in seconds.
  double cleanupTime = 0;

  /// Time spent resetting logical block protection, in seconds.
  double lbpResetTime = 0;

  /// Time spent waiting for the drive to become ready during cleanup, in seconds.
  double readinessWaitTime = 0;

  /// Time spent rewinding during cleanup, in seconds.
  double rewindTime = 0;

  /// Time spent checking and reading the volume label during cleanup, in seconds.
  double labelReadTime = 0;

  /// Time spent clearing encryption during cleanup, in seconds.
  double encryptionControlTime = 0;

  /// Accumulate cleanup durations, including retries.
  void add(const TapeCleanupStats& other) {
    unloadTime += other.unloadTime;
    unmountTime += other.unmountTime;
    cleanupTime += other.cleanupTime;
    lbpResetTime += other.lbpResetTime;
    readinessWaitTime += other.readinessWaitTime;
    rewindTime += other.rewindTime;
    labelReadTime += other.labelReadTime;
    encryptionControlTime += other.encryptionControlTime;
  }
};

/// Statistics for loading and preparing a tape for transfer.
/// Durations accumulate across contributions; component timings may overlap the mounting interval.
struct TapeSetupStats {
  /// Elapsed mounting interval, including loading and initial drive checks, in seconds.
  double mountTime = 0;

  /// Time spent asking the library to mount the cartridge, in seconds.
  double initialMountTime = 0;

  /// Time spent waiting for the mounted cartridge to load and the drive to become ready, in seconds.
  double tapeLoadTime = 0;

  /// Time spent enabling encryption, in seconds.
  double encryptionControlTime = 0;

  /// Initial tape-session preparation, including label checks, LBP setup and positioning, in seconds.
  double positionTime = 0;

  /// Accumulate setup durations from another statistics block.
  void add(const TapeSetupStats& other) {
    mountTime += other.mountTime;
    initialMountTime += other.initialMountTime;
    tapeLoadTime += other.tapeLoadTime;
    encryptionControlTime += other.encryptionControlTime;
    positionTime += other.positionTime;
  }
};

/// Independent components of one tape session, captured together by the tracker.
struct TapeSessionStats {
  /// Mounting and preparation durations, in seconds.
  TapeSetupStats setup;
  /// Tape transfer durations (seconds), volumes (bytes) and file counts.
  TapeTransferStats tape;
  /// Disk delivery and reporting wait durations, in seconds.
  DiskTransferStats disk;
  /// Drive cleanup durations, in seconds, accumulated across cleanup attempts.
  TapeCleanupStats cleanup;

  /// Reported tape-worker elapsed time in seconds; disk delivery may finish later.
  double totalTime = 0;
};

}  // namespace cta::tape::daemon
