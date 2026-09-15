/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <cstdint>

namespace cta::tape::daemon {

/**
 * Statistics for transferring data to or from tape.
 */
struct TapeTransferStats {
  /** Cumulated positioning time, in seconds. */
  double positionTime = 0;

  /** Cumulated time spent computing checksums */
  double checksumingTime = 0;

  /** Cumulated time spent reading and writing data with the drive (for both data and headers). */
  double readWriteTime = 0;

  /** Cumulated time spent flushing */
  double flushTime = 0;

  /** Cumulated time spent waiting for data blocks. */
  double waitDataTime = 0;

  /** Cumulated time spent waiting for free memory. */
  double waitFreeMemoryTime = 0;

  /** Cumulated time spent by the tape thread waiting for a task. */
  double waitInstructionsTime = 0;

  /** Derived time spent transferring files, excluding loading, positioning, and cleanup. */
  double transferTime() const {
    return checksumingTime + readWriteTime + flushTime + waitDataTime + waitFreeMemoryTime + waitInstructionsTime;
  }

  /** Cumulated data volume (actual payload), in bytes. */
  uint64_t dataVolume = 0;

  /** Cumulated space used by file headers. */
  uint64_t headerVolume = 0;

  /** Count of files actually transfered in the session. */
  uint64_t filesCount = 0;

  /** Count of files coming from repack retrieve request transfered in the session.*/
  uint64_t repackFilesCount = 0;

  /** Count of files coming from user retrieve request transfered in the session.*/
  uint64_t userFilesCount = 0;

  /** Count of files coming from verify-only retrieve requests in the session.*/
  uint64_t verifiedFilesCount = 0;

  /** Count of bytes coming from repack retrieve request transfered in the session.*/
  uint64_t repackBytesCount = 0;

  /** Count of bytes coming from user retrieve request transfered in the session.*/
  uint64_t userBytesCount = 0;

  /** Count of bytes coming from verify-only retrieve requests in the session.*/
  uint64_t verifiedBytesCount = 0;

  static const uint64_t headerVolumePerFile = 3 * 80;
  static const uint64_t trailerVolumePerFile = 3 * 80;

  /** Accumulate contents of another stats block */
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

/**
 * Statistics for disk transfers.
 */
struct DiskTransferStats {
  double deliveryTime = 0;
  double waitReportingTime = 0;

  void add(const DiskTransferStats& other) {
    // deliveryTime is elapsed wall-clock time and is not cumulative between threads.
    waitReportingTime += other.waitReportingTime;
  }
};

/** Statistics for restoring the drive and returning the cartridge to the library. */
struct TapeCleanupStats {
  /** Unload time, in seconds. */
  double unloadTime = 0;

  /** Unmount time, in seconds. */
  double unmountTime = 0;

  /** Total elapsed cleanup time, including preparation and recovery retries, in seconds. */
  double cleanupTime = 0;

  /** Time spent resetting logical block protection, in seconds. */
  double lbpResetTime = 0;

  /** Time spent waiting for the drive to become ready during cleanup, in seconds. */
  double readinessWaitTime = 0;

  /** Time spent rewinding during cleanup, in seconds. */
  double rewindTime = 0;

  /** Time spent checking and reading the volume label during cleanup, in seconds. */
  double labelReadTime = 0;

  /** Time spent clearing encryption during cleanup, in seconds. */
  double encryptionControlTime = 0;

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

/** Statistics for loading and preparing a tape for transfer. */
struct TapeSetupStats {
  /** Existing elapsed mounting interval, including loading and initial drive checks, in seconds. */
  double mountTime = 0;

  /** Time spent asking the library to mount the cartridge, in seconds. */
  double initialMountTime = 0;

  /** Time spent waiting for the mounted cartridge to load and the drive to become ready, in seconds. */
  double tapeLoadTime = 0;

  /** Time spent enabling encryption, in seconds. */
  double encryptionControlTime = 0;

  /** Initial tape-session preparation, including label checks, LBP setup and positioning, in seconds. */
  double positionTime = 0;

  void add(const TapeSetupStats& other) {
    mountTime += other.mountTime;
    initialMountTime += other.initialMountTime;
    tapeLoadTime += other.tapeLoadTime;
    encryptionControlTime += other.encryptionControlTime;
    positionTime += other.positionTime;
  }
};

/** Independent components of one tape session, captured together by the tracker. */
struct TapeSessionStats {
  TapeSetupStats setup;
  TapeTransferStats tape;
  DiskTransferStats disk;
  TapeCleanupStats cleanup;

  /** Measured elapsed session time, in seconds. Never accumulated across workers. */
  double totalTime = 0;
};

}  // namespace cta::tape::daemon
