/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

namespace castor::tape::tapeserver::daemon {

/**
   * Structure holding the timers and stats for the tape session. We use doubles,
   * for time and all measurements are in seconds or uint64_t for bytes.
   */
struct TapeSessionStats {
  /** Mounting time, in seconds */
  double mountTime = 0;

  /** Cumulated positioning time, in seconds. */
  double positionTime = 0;

  /** Cumulated time spent computing checksums */
  double checksumingTime = 0;

  /** Cumulated time spent reading and writing data with the drive (for both data and headers). */
  double readWriteTime = 0;

  /** Cumulated time spent flushing */
  double flushTime = 0;

  /** Unload time, in seconds. */
  double unloadTime = 0;

  /** Unmount time, in seconds. */
  double unmountTime = 0;

  /** Time spent running encryption control scripts */
  double encryptionControlTime = 0;

  /** Cumulated time spent waiting for data blocks. */
  double waitDataTime = 0;

  /** Cumulated time spent waiting for free memory. */
  double waitFreeMemoryTime = 0;

  /** Cumulated time spent by the tape thread waiting for a task. */
  double waitInstructionsTime = 0;

  /** Cumulated time spent reporting */
  double waitReportingTime = 0;

  /** Time spent during the session, except mounting, positioning and
     * unloading / unmounting. This a derived value */
  double transferTime() const {
    return checksumingTime + readWriteTime + flushTime + waitDataTime + waitFreeMemoryTime + waitInstructionsTime
           + waitReportingTime;
  }

  /** Total time of the session, computed in parallel */
  double totalTime = 0;

  /** Time to delivery data to the client equal disk threads totalTime
     * for recall and the tape thread totalTime for migration
     */
  double deliveryTime = 0;

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
  void add(const TapeSessionStats& other) {
    mountTime += other.mountTime;
    positionTime += other.positionTime;
    checksumingTime += other.checksumingTime;
    readWriteTime += other.readWriteTime;
    flushTime += other.flushTime;
    unloadTime += other.unloadTime;
    unmountTime += other.unmountTime;
    encryptionControlTime += other.encryptionControlTime;
    waitDataTime += other.waitDataTime;
    waitFreeMemoryTime += other.waitFreeMemoryTime;
    waitInstructionsTime += other.waitInstructionsTime;
    waitReportingTime += other.waitReportingTime;
    // totalTime is not cumulative between threads (it's real time)
    // deliveryTime is not cumulative between thread
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

}  // namespace castor::tape::tapeserver::daemon
