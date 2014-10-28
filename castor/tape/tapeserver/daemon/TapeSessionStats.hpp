/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2014  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

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
    
    /** Cumulated time spent transfering data with the drive (for both data and headers). */
    double transferTime;
    
    /** Cumulated time spent flushing */
    double flushTime;
    
    /** Unload time, in seconds. */
    double unloadTime;
    
    /** Unmount time, in seconds. */
    double unmountTime;
    
    /** Cumulated time spent waiting for data blocks. */
    double waitDataTime;
    
    /** Cumulated time spent waiting for free memory. */
    double waitFreeMemoryTime;
    
    /** Cumulated time spent by the tape thread waiting for a task. */
    double waitInstructionsTime;
    
    /** Cumulated time spent reporting */
    double waitReportingTime;
    
    /** Total time of the session, computed in parallel */
    double totalTime;
    
    /** Cumulated data volume (actual payload), in bytes. */
    uint64_t dataVolume;
    
    /** Cumulated space used by file headers. */
    uint64_t headerVolume;
    
    /** Count of files actually transfered in the session. */
    uint64_t filesCount;
    
    static const uint64_t headerVolumePerFile = 3*80;
    
    /** Constructor: all defaults are zero */
    TapeSessionStats():  mountTime(0.0), positionTime(0.0), checksumingTime(0.0),
    transferTime(0.0), flushTime(0.0), unloadTime(0.0), unmountTime(0.0),
    waitDataTime(0.0), waitFreeMemoryTime(0.0), waitInstructionsTime(0.0),
    waitReportingTime(0.0), totalTime(0.0), dataVolume(0), headerVolume(0),
    filesCount(0) {}
    
    /** Accumulate contents of another stats block */
    void add(const TapeSessionStats& other) {
      mountTime += other.mountTime;
      positionTime += other.positionTime;
      checksumingTime += other.checksumingTime;
      transferTime += other.transferTime;
      flushTime += other.flushTime;
      unloadTime += other.unloadTime;
      unmountTime += other.unmountTime;
      waitDataTime += other.waitDataTime;
      waitFreeMemoryTime += other.waitFreeMemoryTime;
      waitInstructionsTime += other.waitInstructionsTime;
      waitReportingTime += other.waitReportingTime;
      // totalTime is not cumulative between threads (it's real time)
      dataVolume += other.dataVolume;
      headerVolume += other.headerVolume;
      filesCount += other.filesCount;
    }
  };
  
}}}}
