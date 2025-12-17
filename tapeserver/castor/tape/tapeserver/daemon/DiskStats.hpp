/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <string>

namespace castor::tape::tapeserver::daemon {

/**
   * Structure holding the timers and stats for the tape session. We use doubles,
   * for time and all measurements are in seconds or uint64_t for bytes.
   */
struct DiskStats {
  /** Mounting time, in seconds */
  double openingTime = 0;

  /** Closing time, in seconds */
  double closingTime = 0;

  /** Cumulated time spent computing checksums */
  double checksumingTime = 0;

  /** Cumulated time spent calling read/write methods on the disk client */
  double readWriteTime = 0;

  /** Cumulated time spent waiting for data blocks. */
  double waitDataTime = 0;

  /** Cumulated time spent waiting for free memory. */
  double waitFreeMemoryTime = 0;

  /** Cumulated time spent by the tape thread waiting for a task. */
  double waitInstructionsTime = 0;

  /** Cumulated time spent reporting */
  double waitReportingTime = 0;

  /** Cumulated time spent reporting */
  double checkingErrorTime = 0;

  /** Time spent between the opening of the file and the completion of the transfer */
  double transferTime = 0;

  /** Total real time spent by the thread/pool. */
  double totalTime = 0;

  /** Cumulated data volume (actual payload), in bytes. */
  uint64_t dataVolume = 0;

  /** Count of files actually transfered in the session. */
  uint64_t filesCount = 0;

  /** Archive file ID of the current file */
  uint64_t fileId = 0;

  /** Destination URL for the current file */
  std::string dstURL;

  /** Accumulate contents of another stats block */

  void operator+=(const DiskStats& other) {
    openingTime += other.openingTime;
    closingTime += other.closingTime;
    checksumingTime += other.checksumingTime;
    readWriteTime += other.readWriteTime;
    waitDataTime += other.waitDataTime;
    waitFreeMemoryTime += other.waitFreeMemoryTime;
    waitInstructionsTime += other.waitInstructionsTime;
    waitReportingTime += other.waitReportingTime;
    checkingErrorTime += other.checkingErrorTime;
    transferTime +=
      other
        .transferTime;  // Transfer times can be cumulated and used to compute the average file bandwidth over a disk pool
    // total time is not a cumulative member. It is used to represent real time.
    filesCount += other.filesCount;
    dataVolume += other.dataVolume;
  }
};

}  // namespace castor::tape::tapeserver::daemon
