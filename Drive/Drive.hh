// ----------------------------------------------------------------------
// File: Drive/Drive.hh
// Author: Eric Cano - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * Tape Server                                                          *
 * Copyright (C) 2013 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#pragma once

#include "../SCSI/Device.hh"
#include "../SCSI/Structures.hh"
#include "../SCSI/Exception.hh"
#include "../System/Wrapper.hh"
#include "../Exception/Exception.hh"

/**
 * Class wrapping the tape server. Has to be templated (and hence fully in .hh)
 * to allow unit testing against system wrapper.
 */
namespace Tape {

  /**
   * Compressions statistics container, returned by Drive::getCompression()
   */
  class compressionStats {
  public:

    compressionStats() :
    fromHost(0), fromTape(0), toHost(0), toTape(0) {
    }
    uint64_t fromHost;
    uint64_t fromTape;
    uint64_t toHost;
    uint64_t toTape;
  };

  /**
   * Device information, returned by getDeviceInfo()
   */
  class deviceInfo {
  public:
    std::string vendor;
    std::string product;
    std::string productRevisionLevel;
    std::string serialNumber;
  };

  /**
   * Position info, returning both the logical position and the 
   * buffer writing status.
   * Returned by getPositionInfo()
   */
  class positionInfo {
  public:
    uint32_t currentPosition;
    uint32_t oldestDirtyObject;
    uint32_t dirtyObjectsCount;
    uint32_t dirtyBytesCount;
  };

  /**
   * 
   */
  class driveStatus {
    bool ready;
    bool writeProtection;
    /* TODO: Error condition */
    bool eod;
    bool bot;
  };

  class tapeError {
    std::string error;
    /* TODO: error code. See gettperror and get_sk_msg in CAStor */
  };

  /**
   * Class abstracting the tape drives. This class is templated to allow the use
   * of unrelated test harness and real system. The test harness is made up of 
   * a classes with virtual tables, but the real system wrapper has the real
   * system call directly into inline functions. This allows testing on a "fake"
   * system without paying performance price when calling system calls in the 
   * production system.
   */
  class Drive {
  public:
    Drive(SCSI::DeviceInfo di, System::virtualWrapper & sw);

    /* Operations to be used by the higher levels */

    /**
     * Return comulative log counter values from the log pages related to
     * the drive statistics about data movements to/from the tape. 
     * Data fields fromHost, toDrive are related to the write operation and
     * fields toHost, fromDrive are related to the read operation.
     * @return compressionStats
     */
    virtual compressionStats getCompression() throw (Exception) {
      throw Exception("getCompression unavailable in Tape::Drive generic version");
    }

    /**
     * Reset all statistics about data movements on the drive.
     * All comulative and threshold log counter values will be reset to their
     * default values as specified in that pages reset behavior section.
     */
    virtual void clearCompressionStats() throw (Exception);

    /**
     * Information about the drive. The vendor id is used in the user labels of the files.
     * @return    The deviceInfo structure with the information about the drive.
     */
    virtual deviceInfo getDeviceInfo() throw (Exception);

    /**
     * Information about the serial number of the drive. 
     * @return   Right-aligned ASCII data for the vendor-assigned serial number.
     */
    virtual std::string getSerialNumber() throw (Exception);

    /**
     * Position to logical object identifier (i.e. block address). 
     * This function is blocking: the immediate bit is not set.
     * The device server will not return status until the locate operation
     * has completed.
     * @param blockId The blockId, represented in local endianness.
     */
    virtual void positionToLogicalObject(uint32_t blockId) throw (Exception);

    /**
     * Return logical position of the drive. This is the address of the next object
     * to read or write.
     * @return positionInfo class. This contains the logical position, plus information
     * on the dirty data still in the write buffer.
     */
    virtual positionInfo getPositionInfo() throw (Exception);

    /**
     * Get tape alert information from the drive. There is a quite long list of possible tape alerts.
     * They are described in SSC-4, section 4.2.20: TapeAlert application client interface
     * @return list of tape alerts descriptions. They are simply used for logging.
     */
    virtual std::vector<std::string> getTapeAlerts() throw (Exception);

    /**
     * Set the tape density and compression. 
     * We use MODE SENSE/SELECT Device Configuration (10h) mode page.
     * As soon as there is no definition in SPC-4 or SSC-3 it depends on the 
     * drives documentation. 
     * 
     * @param densityCode  The tape specific density code.
     *                     If it is 0 (default) than we use the density code 
     *                     detected by the drive itself means no changes.
     *                                
     * @param compression  The boolean variable to enable or disable compression
     *                     on the drive for the tape. By default it is enabled.
     */
    virtual void setDensityAndCompression(unsigned char densityCode = 0,
            bool compression = true) throw (Exception);

    /**
     * Get drive status.
     * @return structure containing various booleans, and error conditions.
     */
    virtual driveStatus getDriveStatus() throw (Exception) {
      throw Exception("Not implemented");
    }

    /**
     * getTapeError: get SENSE buffer from patched version of the driver
     * or fall back to other information and report tape statuses.
     * Statuses were in CAStor struct sk_info sk_codmsg[] = { 
     * 	{"No sense", ETNOSNS},
     *  {"Recovered error", 0},
     *  {"Not ready", 0},
     *  {"Medium error", ETPARIT},
     *  {"Hardware error", ETHWERR},
     *  {"Illegal request", ETHWERR},
     *  {"Unit attention", ETHWERR},
     *  {"Data protect", 0},
     *  {"Blank check", ETBLANK},
     *  {"Vendor unique", 0},
     *  {"Copy aborted", 0},
     *  {"Aborted command", 0},
     *  {"Equal", 0},
     *  {"Volume overflow", ENOSPC},
     *  {"Miscompare", 0},
     *  {"Reserved", 0},
     *  {"SCSI handshake failure", ETHWERR},
     *  {"Timeout", ETHWERR},
     *  {"EOF hit", 0},
     *  {"EOT hit", ETBLANK},
     *  {"Length error", ETCOMPA},
     *  {"BOT hit", ETUNREC},
     *  {"Wrong tape media", ETCOMPA}
     * @return error code and string containing the error description
     */
    virtual tapeError getTapeError() throw (Exception) {
      throw Exception("Not implemented");
    }

    /**
     * Set the buffer write switch in the st driver. This is directly matching a configuration
     * parameter in CASTOR, so this function has to be public and usable by a higher level
     * layer, unless the parameter turns out to be disused.
     * @param bufWrite: value of the buffer write switch
     */
    virtual void setSTBufferWrite(bool bufWrite) throw (Exception);

    /**
     * Set the MTFastEOM option of the ST driver. This function is used only internally in 
     * mounttape (in CAStor), so it could be a private function, not visible to 
     * the higher levels of the software (TODO: protected?).
     * @param fastMTEOM the option switch.
     */
    virtual void setSTFastMTEOM(bool fastMTEOM) throw (Exception);

    /**
     * Jump to end of media. This will use setSTFastMTEOM() to disable MT_ST_FAST_MTEOM.
     * (See TapeServer's handbook for details). This is used to rebuild the MIR (StorageTek)
     * or tape directory (IBM).
     * Tape directory rebuild is described only for IBM but currently applied to 
     * all tape drives.
     * TODO: synchronous? Timeout?
     */
    virtual void fastSpaceToEOM(void) throw (Exception) {
      throw Exception("Not implemented");
    }

    /**
     * Rewind tape.
     */
    virtual void rewind(void) throw (Exception) {
      throw Exception("Not implemented");
    }

    /**
     * Jump to end of data. EOM in ST driver jargon, end of data (which is more accurate)
     * in SCSI terminology).
     */
    virtual void spaceToEOM(void) throw (Exception) {
      throw Exception("Not implemented");
    }

    /**
     * Space count files backwards.
     * @param count
     */
    virtual void spaceFilesBackward(size_t count) throw (Exception) {
      throw Exception("Not implemented");
    }

    /**
     * Space count files forward.
     * @param count
     */
    virtual void spaceFilesForward(size_t count) throw (Exception) {
      throw Exception("Not implemented");
    }

    /**
     * Space count blocks backwards.
     * @param count
     */
    virtual void spaceBlocksBackwards(size_t count) throw (Exception) {
      throw Exception("Not implemented");
    }

    /**
     * Space count blocks forward.
     * @param count
     */
    virtual void spaceBlocksForward(size_t count) throw (Exception) {
      throw Exception("Not implemented");
    }

    /**
     * Unload the tape.
     */
    virtual void unloadTape(void) throw (Exception) {
      throw Exception("Not implemented");
    }

    /**
     * Synch call to the tape drive. This function will not return before the 
     * data in the drive's buffer is actually comitted to the medium.
     */
    virtual void sync(void) throw (Exception) {
      throw Exception("Not implemented");
    }

    /**
     * Write count file marks. The function does not return before the file marks 
     * are committed to medium.
     * @param count
     */
    virtual void writeSyncFileMarks(size_t count) throw (Exception) {
      throw Exception("Not implemented");
    }

    /**
     * Write count file marks asynchronously. The file marks are just added to the drive's
     * buffer and the function return immediately.
     * @param count
     */
    virtual void writeImmediateFileMarks(size_t count) throw (Exception) {
      throw Exception("Not implemented");
    }

    /**
     * Write a data block to tape.
     * @param data pointer the the data block
     * @param count size of the data block
     */
    virtual void writeBlock(const unsigned char * data, size_t count) throw (Exception) {
      throw Exception("Not implemented");
    }

    /**
     * Read a data block from tape.
     * @param data pointer the the data block
     * @param count size of the data block
     */
    virtual void readBlock(unsigned char * data, size_t count) throw (Exception) {
      throw Exception("Not implemented");
    }

    virtual ~Drive() {
      if (-1 != m_tapeFD)
        m_sysWrapper.close(m_tapeFD);
      if (-1 != m_genericFD)
        m_sysWrapper.close(m_genericFD);
    }

    void SCSI_inquiry();
  protected:
    SCSI::DeviceInfo m_SCSIInfo;
    int m_tapeFD;
    int m_genericFD;
    Tape::System::virtualWrapper & m_sysWrapper;
    struct mtget m_mtInfo;
  private:

    void SCSI_inquiry(int fd);
  };

  class DriveT10000 : public Drive {
  public:

    DriveT10000(SCSI::DeviceInfo di, System::virtualWrapper & sw) : Drive(di, sw) {
    }

    virtual compressionStats getCompression() throw (Exception);
  };

  class DriveLTO : public Drive {
  public:

    DriveLTO(SCSI::DeviceInfo di, System::virtualWrapper & sw) : Drive(di, sw) {
    }

    virtual compressionStats getCompression() throw (Exception);
  };

  class DriveIBM3592 : public Drive {
  public:

    DriveIBM3592(SCSI::DeviceInfo di, System::virtualWrapper & sw) : Drive(di, sw) {
    }

    virtual compressionStats getCompression() throw (Exception);
  };
}
