/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
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

#include "castor/exception/Errnum.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/tape/tapeserver/daemon/DriveConfig.hpp"
#include "castor/tape/tapeserver/drive/mtio_add.hpp"
#include "castor/tape/tapeserver/SCSI/Device.hpp"
#include "castor/tape/tapeserver/SCSI/Exception.hpp"
#include "castor/tape/tapeserver/SCSI/Structures.hpp"
#include "castor/tape/tapeserver/system/Wrapper.hpp"

/**
 * Class wrapping the tape server. Has to be templated (and hence fully in .hh)
 * to allow unit testing against system wrapper.
 */
namespace castor {
namespace tape {
namespace tapeserver {
namespace drive {

  /**
   * Compressions statistics container, returned by Drive::getCompression()
   */
  class compressionStats {
  public:

    compressionStats() :
    fromHost(0),toTape(0) ,fromTape(0), toHost(0) {
    }
    //migration stats
    
    //amount of bytes the host sent
    uint64_t fromHost;
    
    //amount of bytes really wrote on byte
    uint64_t toTape;
    //--------------------------------------------------------------------------
    //recall stats : currently filled by the drive but unused elsewhere
    //amount of bytes the drive read on tape
    uint64_t fromTape;
    
    //amount of bytes we send to the client
    uint64_t toHost;
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
    bool isPIsupported;
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
   * Logical block protection nformation, returned by getLBPInfo()
   */
  class LBPInfo {
  public:
    unsigned char method;
    unsigned char methodLength;
    bool enableLBPforRead;
    bool enableLBPforWrite;
  };

  /**
   * enum class to be used for specifying LBP method to use on the drive.
   */
  enum class lbpToUse {
    disabled,
    crc32cReadWrite,
    crc32cReadOnly
  };
  
  /**
   * 
   */
  struct driveStatus {
    bool ready;
    bool writeProtection;
    /* TODO: Error condition */
    bool eod;
    bool bot;
    bool hasTapeInPlace;
  };

  class tapeError {
    std::string error;
    /* TODO: error code. See gettperror and get_sk_msg in CAStor */
  };
  
  /**
   * Exception reported by drive functions when trying to read beyond
   * end of data
   */
  class EndOfData: public castor::exception::Exception {
  public:
    EndOfData(const std::string w=""): Exception(w) {}
  };
  
  /**
   * Exception reported by drive functions when trying to write beyond
   * end of medium
   */
  class EndOfMedium: public castor::exception::Exception {
  public:
    EndOfMedium(const std::string w=""): Exception(w) {}
  };
  
  /**
   * Exception reported by ReadExactBlock when the size is not right
   */
  class UnexpectedSize: public castor::exception::Exception {
  public:
    UnexpectedSize(const std::string w=""): Exception(w) {}
  };

  /**
   * Exception reported by ReadFileMark when finding a data block
   */
  class NotAFileMark: public castor::exception::Exception {
  public:
    NotAFileMark(const std::string w=""): Exception(w) {}
  };
  
  /**
   * Abstract class used by DriveGeneric(real stuff) and the FakeDrive(used for unit testing)
   */
  class DriveInterface {
  public:
    virtual ~DriveInterface(){};
    
    virtual compressionStats getCompression()  = 0;
    virtual void clearCompressionStats()  = 0;
    
    virtual deviceInfo getDeviceInfo()  = 0;
    virtual std::string getSerialNumber()  = 0;
    virtual void positionToLogicalObject(uint32_t blockId)  = 0;
    virtual positionInfo getPositionInfo()  = 0;
    virtual std::vector<uint16_t> getTapeAlertCodes() = 0;
    virtual std::vector<std::string> getTapeAlerts(const std::vector<uint16_t>&) = 0;
    virtual std::vector<std::string> getTapeAlertsCompact (const std::vector<uint16_t>&) = 0;
    virtual void setDensityAndCompression(bool compression = true,
    unsigned char densityCode = 0)  = 0;
    virtual void enableCRC32CLogicalBlockProtectionReadOnly() = 0;
    virtual void enableCRC32CLogicalBlockProtectionReadWrite() = 0;
    virtual void enableReedSolomonLogicalBlockProtectionReadOnly() = 0;
    virtual void enableReedSolomonLogicalBlockProtectionReadWrite() = 0;
    virtual void disableLogicalBlockProtection() = 0;
    virtual drive::LBPInfo getLBPInfo() = 0;
    virtual void setLogicalBlockProtection(const unsigned char method,
      unsigned char methodLength, const bool enableLPBforRead, 
      const bool enableLBBforWrite) = 0; 
    virtual driveStatus getDriveStatus()  = 0;
    virtual tapeError getTapeError()  = 0;
    virtual void setSTBufferWrite(bool bufWrite)  = 0;
    
    virtual void fastSpaceToEOM(void)  = 0;
    virtual void rewind(void)  = 0;
    virtual void spaceToEOM(void)  = 0;
    virtual void spaceFileMarksBackwards(size_t count)  = 0;
    virtual void spaceFileMarksForward(size_t count)  = 0;
    
    virtual void unloadTape(void)  = 0;
    
    virtual void flush(void)  = 0;
    
    virtual void writeSyncFileMarks(size_t count)  = 0;
    virtual void writeImmediateFileMarks(size_t count)  = 0;
    virtual void writeBlock(const void * data, size_t count)  = 0;
    
    virtual ssize_t readBlock(void * data, size_t count)  = 0;
    virtual void readExactBlock(void * data, size_t count, std::string context)  = 0;
    virtual void readFileMark(std::string context)  = 0;
    
    virtual void waitUntilReady(const uint32_t timeoutSecond)  = 0;    
    
    virtual bool isWriteProtected()  = 0;
    virtual bool isAtBOT()  = 0;
    virtual bool isAtEOD()  = 0;
    virtual bool isTapeBlank() = 0;
    virtual lbpToUse getLbpToUse() = 0;
    virtual bool hasTapeInPlace() = 0;
    
    /**
     * The configuration of teh tape drive as parsed from the TPCONFIG file.
     */
    daemon::DriveConfig config;
  };
  
  /**
   * Factory function that allocated the proper drive type, based on device info
   * @param di deviceInfo, as found in a DeviceVector.
   * @param sw reference to the system wrapper.
   * @return pointer to the newly allocated drive object
   */
  
  DriveInterface * createDrive(SCSI::DeviceInfo di,
     System::virtualWrapper & sw);
  
} // namespace drive
} // namespace tapeserver
} // namespace tape
} // namespace castor
