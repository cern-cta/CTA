/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
#pragma once

#include <limits>
#include <list>
#include <map>
#include <string>
#include <vector>

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
  enum FailureMoment {OnWrite, OnFlush};

 private:
  const enum FailureMoment m_failureMoment;
  bool m_tapeOverflow;
  bool m_failToMount;
  lbpToUse m_lbpToUse;

 public:
  std::string contentToString() throw();

  FakeDrive(uint64_t capacity = std::numeric_limits<uint64_t>::max(),
    enum FailureMoment failureMoment = OnWrite,
    bool failOnMount = false) throw();
  explicit FakeDrive(bool failOnMount) throw();
  virtual ~FakeDrive() throw() {}
  virtual compressionStats getCompression();
  virtual void clearCompressionStats();
  virtual std::map<std::string, uint64_t> getTapeWriteErrors();
  virtual std::map<std::string, uint64_t> getTapeReadErrors();
  virtual std::map<std::string, uint32_t> getTapeNonMediumErrors();
  virtual std::map<std::string, float> getQualityStats();
  virtual std::map<std::string, uint32_t> getDriveStats();
  virtual std::map<std::string, uint32_t> getVolumeStats();
  virtual std::string getDriveFirmwareVersion();
  virtual deviceInfo getDeviceInfo();
  virtual std::string getGenericSCSIPath();
  virtual std::string getSerialNumber();
  virtual void positionToLogicalObject(uint32_t blockId);
  virtual positionInfo getPositionInfo();
  virtual physicalPositionInfo getPhysicalPositionInfo();
  virtual std::vector<endOfWrapPosition> getEndOfWrapPositions();
  virtual std::vector<uint16_t> getTapeAlertCodes();
  virtual std::vector<std::string> getTapeAlerts(const std::vector<uint16_t>&);
  virtual std::vector<std::string> getTapeAlertsCompact(const std::vector<uint16_t>&);
  virtual bool tapeAlertsCriticalForWrite(const std::vector<uint16_t> & codes);
  virtual void setDensityAndCompression(bool compression = true,
  unsigned char densityCode = 0);
  virtual void enableCRC32CLogicalBlockProtectionReadOnly();
  virtual void enableCRC32CLogicalBlockProtectionReadWrite();
  virtual void disableLogicalBlockProtection();
  virtual drive::LBPInfo getLBPInfo();
  virtual void setLogicalBlockProtection(const unsigned char method,
    unsigned char methodLength, const bool enableLPBforRead,
    const bool enableLBBforWrite);
  virtual void setEncryptionKey(const std::string &encryption_key);
  virtual bool clearEncryptionKey();
  virtual bool isEncryptionCapEnabled();
  virtual driveStatus getDriveStatus();
  virtual void setSTBufferWrite(bool bufWrite);
  virtual void fastSpaceToEOM(void);
  virtual void rewind(void);
  virtual void spaceToEOM(void);
  virtual void spaceFileMarksBackwards(size_t count);
  virtual void spaceFileMarksForward(size_t count);
  virtual void unloadTape(void);
  virtual void flush(void);
  virtual void writeSyncFileMarks(size_t count);
  virtual void writeImmediateFileMarks(size_t count);
  virtual void writeBlock(const void * data, size_t count);
  virtual ssize_t readBlock(void * data, size_t count);
  virtual void readExactBlock(void * data, size_t count, const std::string& context);
  virtual void readFileMark(const std::string& context);
  virtual void waitUntilReady(const uint32_t timeoutSecond);
  virtual bool isWriteProtected();
  virtual bool isAtBOT();
  virtual bool isAtEOD();
  virtual bool isTapeBlank();
  virtual lbpToUse getLbpToUse();
  virtual bool hasTapeInPlace();
  virtual castor::tape::SCSI::Structures::RAO::udsLimits getLimitUDS();
  virtual void queryRAO(std::list<SCSI::Structures::RAO::blockLims> &files, int maxSupported);
};

class FakeNonRAODrive : public FakeDrive{
 public:
  FakeNonRAODrive();
  virtual castor::tape::SCSI::Structures::RAO::udsLimits getLimitUDS();
};

}  // namespace drive
}  // namespace tapeserver
}  // namespace tape
}  // namespace castor
