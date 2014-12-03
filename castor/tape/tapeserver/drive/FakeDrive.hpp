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

#include "castor/tape/tapeserver/drive/DriveInterface.hpp"


namespace castor {
namespace tape {
namespace tapeserver {
namespace drive {
/**
   * Fake drive class used for unit testing
  */
  class FakeDrive : public DriveInterface {
  private:
    struct tapeBlock {
      std::string data;
      uint64_t remainingSpaceAfter;
    };
    std::vector<tapeBlock> m_tape;
    uint32_t m_currentPosition;
    uint64_t m_tapeCapacity;
    int m_beginOfCompressStats;
    uint64_t getRemaingSpace(uint32_t currentPosition);
  public:
    enum FailureMoment { OnWrite, OnFlush } ;
  private:
    const enum FailureMoment m_failureMoment;
    bool m_tapeOverflow;
    bool m_failToMount;
  public:
    std::string contentToString() throw();

    FakeDrive(uint64_t capacity=std::numeric_limits<uint64_t>::max(),
      enum FailureMoment failureMoment=OnWrite,
      bool failOnMount = false) throw();
    FakeDrive(bool failOnMount) throw ();
    virtual ~FakeDrive() throw(){}
    virtual compressionStats getCompression() ;
    virtual void clearCompressionStats() ;
    virtual deviceInfo getDeviceInfo() ;
    virtual std::string getSerialNumber() ;
    virtual void positionToLogicalObject(uint32_t blockId) ;
    virtual positionInfo getPositionInfo() ;
    virtual std::vector<std::string> getTapeAlerts() ;
    virtual std::vector<std::string> getTapeAlertsCompact() ;
    virtual void setDensityAndCompression(bool compression = true, 
    unsigned char densityCode = 0) ;
    virtual driveStatus getDriveStatus() ;
    virtual tapeError getTapeError() ;
    virtual void setSTBufferWrite(bool bufWrite) ;
    virtual void fastSpaceToEOM(void) ;
    virtual void rewind(void) ;
    virtual void spaceToEOM(void) ;
    virtual void spaceFileMarksBackwards(size_t count) ;
    virtual void spaceFileMarksForward(size_t count) ;
    virtual void unloadTape(void) ;
    virtual void flush(void) ;
    virtual void writeSyncFileMarks(size_t count) ;
    virtual void writeImmediateFileMarks(size_t count) ;
    virtual void writeBlock(const void * data, size_t count) ;
    virtual ssize_t readBlock(void * data, size_t count) ;
    virtual void readExactBlock(void * data, size_t count, std::string context) ;
    virtual void readFileMark(std::string context) ;
    virtual bool waitUntilReady(int timeoutSecond) ;    
    virtual bool isWriteProtected() ;
    virtual bool isAtBOT() ;
    virtual bool isAtEOD() ;
    virtual bool isTapeBlank();
    virtual bool hasTapeInPlace();
  };
  
}}}}
