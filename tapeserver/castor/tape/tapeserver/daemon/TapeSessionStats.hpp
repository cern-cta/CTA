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

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
/**
   * Structure holding the timers and stats for the tape session. We use doubles, 
   * for time and all measurements are in seconds or uint64_t for bytes.
   */
struct TapeSessionStats {
  /** Mounting time, in seconds */
  double mountTime;

  /** Cumulated positioning time, in seconds. */
  double positionTime;

  /** Cumulated time spent computing checksums */
  double checksumingTime;

  /** Cumulated time spent reading and writing data with the drive (for both data and headers). */
  double readWriteTime;

  /** Cumulated time spent flushing */
  double flushTime;

  /** Unload time, in seconds. */
  double unloadTime;

  /** Unmount time, in seconds. */
  double unmountTime;

  /** Time spent running encryption control scripts */
  double encryptionControlTime;

  /** Cumulated time spent waiting for data blocks. */
  double waitDataTime;

  /** Cumulated time spent waiting for free memory. */
  double waitFreeMemoryTime;

  /** Cumulated time spent by the tape thread waiting for a task. */
  double waitInstructionsTime;

  /** Cumulated time spent reporting */
  double waitReportingTime;

  /** Time spent during the session, except mounting, positioning and 
     * unloading / unmounting. This a derived value */
  double transferTime() const {
    return checksumingTime + readWriteTime + flushTime + waitDataTime + waitFreeMemoryTime + waitInstructionsTime +
           waitReportingTime;
  }

  /** Total time of the session, computed in parallel */
  double totalTime;

  /** Time to delivery data to the client equal disk threads totalTime 
     * for recall and the tape thread totalTime for migration
     */
  double deliveryTime;

  /** Cumulated data volume (actual payload), in bytes. */
  uint64_t dataVolume;

  /** Cumulated space used by file headers. */
  uint64_t headerVolume;

  /** Count of files actually transfered in the session. */
  uint64_t filesCount;

  /** Count of files coming from repack retrieve request transfered in the session.*/
  uint64_t repackFilesCount;

  /** Count of files coming from user retrieve request transfered in the session.*/
  uint64_t userFilesCount;

  /** Count of files coming from verify-only retrieve requests in the session.*/
  uint64_t verifiedFilesCount;

  /** Count of bytes coming from repack retrieve request transfered in the session.*/
  uint64_t repackBytesCount;

  /** Count of bytes coming from user retrieve request transfered in the session.*/
  uint64_t userBytesCount;

  /** Count of bytes coming from verify-only retrieve requests in the session.*/
  uint64_t verifiedBytesCount;

  static const uint64_t headerVolumePerFile = 3 * 80;
  static const uint64_t trailerVolumePerFile = 3 * 80;

  /** Constructor: all defaults are zero */
  TapeSessionStats() :
  mountTime(0.0),
  positionTime(0.0),
  checksumingTime(0.0),
  readWriteTime(0.0),
  flushTime(0.0),
  unloadTime(0.0),
  unmountTime(0.0),
  encryptionControlTime(0.0),
  waitDataTime(0.0),
  waitFreeMemoryTime(0.0),
  waitInstructionsTime(0.0),
  waitReportingTime(0.0),
  totalTime(0.0),
  deliveryTime(0.0),
  dataVolume(0),
  headerVolume(0),
  filesCount(0),
  repackFilesCount(0),
  userFilesCount(0),
  verifiedFilesCount(0),
  repackBytesCount(0),
  userBytesCount(0),
  verifiedBytesCount(0) {}

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

}  // namespace daemon
}  // namespace tapeserver
}  // namespace tape
}  // namespace castor
