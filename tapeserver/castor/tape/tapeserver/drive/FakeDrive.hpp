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

#include <limits>
#include <list>
#include <map>
#include <string>
#include <vector>

#include "castor/tape/tapeserver/drive/DriveInterface.hpp"

namespace castor::tape::tapeserver::drive {

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
  uint32_t m_currentPosition = 0;
  uint64_t m_tapeCapacity;
  int m_beginOfCompressStats = 0;
  uint64_t getRemaingSpace(uint32_t currentPosition);

 public:
  enum FailureMoment {OnWrite, OnFlush};

 private:
  const enum FailureMoment m_failureMoment;
  bool m_tapeOverflow = false;
  bool m_failToMount;
  lbpToUse m_lbpToUse;

 public:
  std::string contentToString() noexcept;

  FakeDrive(uint64_t capacity = std::numeric_limits<uint64_t>::max(),
    enum FailureMoment failureMoment = OnWrite,
    bool failOnMount = false) noexcept;
  explicit FakeDrive(bool failOnMount) noexcept;
  ~FakeDrive() override = default;
  compressionStats getCompression() final;
  void clearCompressionStats() final;
  std::map<std::string, uint64_t> getTapeWriteErrors() final;
  std::map<std::string, uint64_t> getTapeReadErrors() final;
  std::map<std::string, uint32_t> getTapeNonMediumErrors() final;
  std::map<std::string, float> getQualityStats() final;
  std::map<std::string, uint32_t> getDriveStats() final;
  std::map<std::string, uint32_t> getVolumeStats() final;
  std::string getDriveFirmwareVersion() final;
  deviceInfo getDeviceInfo() final;
  std::string getGenericSCSIPath() final;
  std::string getSerialNumber() final;
  void positionToLogicalObject(uint32_t blockId) final;
  positionInfo getPositionInfo() final;
  physicalPositionInfo getPhysicalPositionInfo() final;
  std::vector<endOfWrapPosition> getEndOfWrapPositions() final;
  std::vector<uint16_t> getTapeAlertCodes() final;
  std::vector<std::string> getTapeAlerts(const std::vector<uint16_t>&) final;
  std::vector<std::string> getTapeAlertsCompact(const std::vector<uint16_t>&) final;
  bool tapeAlertsCriticalForWrite(const std::vector<uint16_t> & codes) final;
  void setDensityAndCompression(bool compression = true, unsigned char densityCode = 0) final;
  void enableCRC32CLogicalBlockProtectionReadOnly() final;
  void enableCRC32CLogicalBlockProtectionReadWrite() final;
  void disableLogicalBlockProtection() final;
  drive::LBPInfo getLBPInfo() final;
  void setLogicalBlockProtection(const unsigned char method, unsigned char methodLength,
    const bool enableLPBforRead, const bool enableLBBforWrite) final;
  void setEncryptionKey(const std::string &encryption_key) final;
  bool clearEncryptionKey() final;
  bool isEncryptionCapEnabled() final;
  driveStatus getDriveStatus() final;
  void setSTBufferWrite(bool bufWrite) final;
  void fastSpaceToEOM(void) final;
  void rewind(void) final;
  void spaceToEOM(void) final;
  void spaceFileMarksBackwards(size_t count) final;
  void spaceFileMarksForward(size_t count) final;
  void unloadTape(void) final;
  void flush(void) final;
  void writeSyncFileMarks(size_t count) final;
  void writeImmediateFileMarks(size_t count) final;
  void writeBlock(const void * data, size_t count) final;
  ssize_t readBlock(void * data, size_t count) final;
  void readExactBlock(void * data, size_t count, const std::string& context) final;
  void readFileMark(const std::string& context) final;
  void waitUntilReady(const uint32_t timeoutSecond) final;
  bool isWriteProtected() final;
  bool isAtBOT() final;
  bool isAtEOD() final;
  bool isTapeBlank() final;
  lbpToUse getLbpToUse() final;
  bool hasTapeInPlace() final;
  castor::tape::SCSI::Structures::RAO::udsLimits getLimitUDS() override;
  void queryRAO(std::list<SCSI::Structures::RAO::blockLims> &files, int maxSupported) final;
};

class FakeNonRAODrive : public FakeDrive{
 public:
  FakeNonRAODrive();
  castor::tape::SCSI::Structures::RAO::udsLimits getLimitUDS() final;
};

} // namespace castor::tape::tapeserver::drive
