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
#include "castor/tape/tapeserver/SCSI/Device.hpp"
#include "castor/tape/tapeserver/SCSI/Structures.hpp"
#include "castor/tape/tapeserver/SCSI/Exception.hpp"
#include "castor/tape/tapeserver/drive/mtio_add.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/tape/tapeserver/system/Wrapper.hpp"

/**
 * Class wrapping the tape server. Has to be templated (and hence fully in .hh)
 * to allow unit testing against system wrapper.
 */
namespace castor {
namespace tape {
namespace tapeserver {
namespace drives {

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
    virtual std::vector<std::string> getTapeAlerts()  = 0;
    virtual void setDensityAndCompression(bool compression = true,
    unsigned char densityCode = 0)  = 0;
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
    virtual bool waitUntilReady(int timeoutSecond)  = 0;    
    virtual bool isWriteProtected()  = 0;
    virtual bool isAtBOT()  = 0;
    virtual bool isAtEOD()  = 0;
    virtual bool isTapeBlank() = 0;
    virtual bool hasTapeInPlace() = 0;
    /**
     * Member string allowing the convenient storage of the string describing
     * drive location for the mount system (we get the information from TPCONFIG
     */
    std::string librarySlot;
  };
  
  /**
   * Factory function that allocated the proper drive type, based on device info
   * @param di deviceInfo, as found in a DeviceVector.
   * @param sw reference to the system wrapper.
   * @return pointer to the newly allocated drive object
   */
  
  DriveInterface * DriveFactory(SCSI::DeviceInfo di,
     System::virtualWrapper & sw);
  
} // namespace drives
} // namespace tapeserver
} // namespace tape
} // namespace castor
