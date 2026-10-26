/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "DriveInterface.hpp"

#include <limits>
#include <list>
#include <map>
#include <string>
#include <vector>

namespace cta::tape::drive {

/**
 * In-memory tape drive implementation.  Simulates a tape as a vector of
 * logical blocks in RAM; all positioning, read, and write operations run
 * against this vector without any SCSI I/O or kernel involvement.
 *
 * Subclasses add specialised behaviour:
 *
 *   FakeDrive   — configurable failure injection for unit tests only.
 *   StressDrive — on-disk persistence (PVC-backed *.tape files) so tapes
 *                 survive mount/dismount cycles and are shared across
 *                 drives, enabling full-scale CI testing without mhVTL.
 *
 * Comparison with mhVTL:
 *
 *   Property             mhVTL                  VirtualDrive / StressDrive
 *   Backend              SCSI target (kernel)   Filesystem directory (PVC)
 *   Tiny-file latency    High (SCSI + driver)   Minimal (direct file I/O)
 *   Drive count          Limited by SCSI tgts   Unlimited (in-process)
 *   CI node setup        Kernel module needed   Zero (any k8s node)
 *   Mount delay          Fixed, hardware-speed  Configurable / jittered
 *   Suitable for         Hardware-fidelity       Metadata & scale tests
 *
 * Methods without a meaningful virtual-tape implementation
 * (getSerialNumber, getDriveStatus, setDensityAndCompression, getLBPInfo,
 * setSTBufferWrite) throw NotImplementedException and must be overridden by
 * subclasses that need them.
 */
class VirtualDrive : public DriveInterface {
protected:
  // One logical record on the simulated tape.  A file mark has an empty data
  // string; a data block has non-empty data.
  struct tapeBlock {
    std::string data;
    uint64_t remainingSpaceAfter;
  };

  std::vector<tapeBlock> m_tape;
  uint32_t m_currentPosition = 0;
  uint64_t m_tapeCapacity;
  lbpToUse m_lbpToUse = lbpToUse::disabled;
  bool m_tapeInPlace = true;

public:
  explicit VirtualDrive(uint64_t capacity = std::numeric_limits<uint64_t>::max()) noexcept;
  ~VirtualDrive() override = default;

  compressionStats getCompression() override;
  void clearCompressionStats() override;
  std::map<std::string, uint64_t> getTapeWriteErrors() override;
  std::map<std::string, uint64_t> getTapeReadErrors() override;
  std::map<std::string, uint32_t> getTapeNonMediumErrors() override;
  std::map<std::string, float> getQualityStats() override;
  std::map<std::string, uint32_t> getDriveStats() override;
  std::map<std::string, uint32_t> getVolumeStats() override;
  std::string getDriveFirmwareVersion() override;
  deviceInfo getDeviceInfo() override;
  std::string getGenericSCSIPath() override;
  std::string getSerialNumber() override;
  void positionToLogicalObject(uint32_t blockId) override;
  positionInfo getPositionInfo() override;
  physicalPositionInfo getPhysicalPositionInfo() override;
  std::vector<endOfWrapPosition> getEndOfWrapPositions() override;
  std::vector<uint16_t> getTapeAlertCodes() override;
  std::vector<std::string> getTapeAlerts(const std::vector<uint16_t>&) override;
  std::vector<std::string> getTapeAlertsCompact(const std::vector<uint16_t>&) override;
  bool tapeAlertsCriticalForWrite(const std::vector<uint16_t>& codes) override;
  void setDensityAndCompression(bool compression = true, unsigned char densityCode = 0) override;
  void enableCRC32CLogicalBlockProtectionReadOnly() override;
  void enableCRC32CLogicalBlockProtectionReadWrite() override;
  void disableLogicalBlockProtection() override;
  drive::LBPInfo getLBPInfo() override;
  void setLogicalBlockProtection(const unsigned char method,
                                 unsigned char methodLength,
                                 const bool enableLPBforRead,
                                 const bool enableLBBforWrite) override;
  void setEncryptionKey(const std::string& encryption_key) override;
  bool clearEncryptionKey() override;
  bool isEncryptionCapEnabled() override;
  driveStatus getDriveStatus() override;
  void setSTBufferWrite(bool bufWrite) override;
  void fastSpaceToEOM(void) override;
  void rewind(void) override;
  void spaceToEOM(void) override;
  void spaceFileMarksBackwards(size_t count) override;
  void spaceFileMarksForward(size_t count) override;
  void unloadTape(void) override;
  void flush(void) override;
  void writeSyncFileMarks(size_t count) override;
  void writeImmediateFileMarks(size_t count) override;
  void writeBlock(const void* data, size_t count) override;
  ssize_t readBlock(void* data, size_t count) override;
  void readExactBlock(void* data, size_t count, const std::string& context) override;
  void readFileMark(const std::string& context) override;
  void waitUntilReady(const uint32_t timeoutSecond) override;
  bool isWriteProtected() override;
  bool isAtBOT() override;
  bool isAtEOD() override;
  bool isTapeBlank() override;
  lbpToUse getLbpToUse() override;
  bool hasTapeInPlace() override;
  cta::tape::SCSI::Structures::RAO::udsLimits getLimitUDS() override;
  void queryRAO(std::list<SCSI::Structures::RAO::blockLims>& files, int maxSupported) override;

  void setTapeInPlace(bool tapeInPlace);
  uint32_t getBlockIdPositioningCount() const;

protected:
  uint64_t getRemainingSpace(uint32_t currentPosition);

private:
  int m_beginOfCompressStats = 0;
  uint32_t m_blockIdPositioningCount = 0;
};

}  // namespace cta::tape::drive
