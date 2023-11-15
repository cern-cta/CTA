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
  struct DiskStats {
    /** Mounting time, in seconds */
    double openingTime;
    
    /** Closing time, in seconds */
    double closingTime;
    
    /** Cumulated time spent computing checksums */
    double checksumingTime;
    
    /** Cumulated time spent calling read/write methods on the disk client */
    double readWriteTime;
    
    /** Cumulated time spent waiting for data blocks. */
    double waitDataTime;
    
    /** Cumulated time spent waiting for free memory. */
    double waitFreeMemoryTime;
    
    /** Cumulated time spent by the tape thread waiting for a task. */
    double waitInstructionsTime;
    
    /** Cumulated time spent reporting */
    double waitReportingTime;
  
    /** Cumulated time spent reporting */
    double checkingErrorTime;
    
    /** Time spent between the opening of the file and the completion of the transfer */
    double transferTime;
    
    /** Total real time spent by the thread/pool. */
    double totalTime;
    
    /** Cumulated data volume (actual payload), in bytes. */
    uint64_t dataVolume;
    
    /** Count of files actually transfered in the session. */
    uint64_t filesCount;
    
    /** Archive file ID of the current file */
    uint64_t fileId;

    /** Destination URL for the current file */
    std::string dstURL;

    /** Constructor: all defaults are zero */
    DiskStats():  openingTime(0.0), 
    closingTime(0.0),
    checksumingTime(0.0),
    readWriteTime(0.0),
    waitDataTime(0.0), 
    waitFreeMemoryTime(0.0),
    waitInstructionsTime(0.0),
    waitReportingTime(0.0), 
    checkingErrorTime(0.0),
    transferTime(0.0),
    totalTime(0.0),
    dataVolume(0),
    filesCount(0),
    fileId(0),
    dstURL("") {}
    
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
      transferTime += other.transferTime; // Transfer times can be cumulated and used to compute the average file bandwidth over a disk pool
      // total time is not a cumulative member. It is used to represent real time.
      filesCount += other.filesCount;
      dataVolume += other.dataVolume;
    }
  };
  
} // namespace castor::tape::tapeserver::daemon

