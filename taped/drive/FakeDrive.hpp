/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "DriveInterface.hpp"
#include "InMemoryTape.hpp"

#include <set>
#include <string>

namespace cta::tape::drive {

/**
 * Fake drive used for unit testing.  Implements DriveInterface directly and
 * holds an InMemoryTape for all tape I/O.  Configurable failure injection is
 * layered on top of the in-memory tape operations.
 */
class FakeDrive : public DriveInterface {
public:
  enum FailureMoment { OnWrite, OnFlush };
  enum class FailurePoint { ClearEncryptionKey, Rewind, DisableLogicalBlockProtection, UnloadTape };

private:
  const enum FailureMoment m_failureMoment;
  std::set<FailurePoint> m_failurePoints;
  bool m_tapeOverflow = false;
  bool m_failToMount;
  uint32_t m_blockIdPositioningCount = 0;
  lbpToUse m_lbpToUse = lbpToUse::disabled;

protected:
  InMemoryTape m_tape;

public:
  explicit FakeDrive(uint64_t capacity = std::numeric_limits<uint64_t>::max(),
                     enum FailureMoment failureMoment = OnWrite,
                     bool failOnMount = false) noexcept;
  explicit FakeDrive(bool failOnMount) noexcept;
  ~FakeDrive() override = default;

  // --- Failure-injecting overrides ---
  void writeBlock(const void* data, size_t count) override;
  void flush(void) override;
  void rewind(void) override;
  void disableLogicalBlockProtection() override;
  bool clearEncryptionKey() override;
  void unloadTape(void) override;
  void waitUntilReady(const uint32_t timeoutSecond) override;

  // --- Test-fixture overrides: return predictable non-zero values so tests
  //     can assert on specific log output without depending on real hardware. ---
  std::map<std::string, uint64_t> getTapeWriteErrors() override;
  std::map<std::string, uint64_t> getTapeReadErrors() override;
  std::map<std::string, uint32_t> getTapeNonMediumErrors() override;
  std::map<std::string, float> getQualityStats() override;
  std::map<std::string, uint32_t> getDriveStats() override;
  std::string getDriveFirmwareVersion() override;
  deviceInfo getDeviceInfo() override;
  std::string getGenericSCSIPath() override;
  std::string getSerialNumber() override;

  // --- Delegation to InMemoryTape (tape-block operations) ---
  compressionStats getCompression() override { return m_tape.getCompression(); }
  void clearCompressionStats() override { m_tape.clearCompressionStats(); }
  void positionToLogicalObject(uint32_t b) override;
  positionInfo getPositionInfo() override { return m_tape.getPositionInfo(); }
  physicalPositionInfo getPhysicalPositionInfo() override { return m_tape.getPhysicalPositionInfo(); }
  std::vector<endOfWrapPosition> getEndOfWrapPositions() override;
  void enableCRC32CLogicalBlockProtectionReadOnly() override;
  void enableCRC32CLogicalBlockProtectionReadWrite() override;
  void disableLogicalBlockProtection() override;
  void fastSpaceToEOM(void) override { m_tape.fastSpaceToEOM(); }
  void spaceToEOM(void) override { m_tape.spaceToEOM(); }
  void spaceFileMarksBackwards(size_t count) override { m_tape.spaceFileMarksBackwards(count); }
  void spaceFileMarksForward(size_t count) override { m_tape.spaceFileMarksForward(count); }
  void writeSyncFileMarks(size_t count) override { m_tape.writeSyncFileMarks(count); }
  void writeImmediateFileMarks(size_t count) override { m_tape.writeImmediateFileMarks(count); }
  ssize_t readBlock(void* data, size_t count) override { return m_tape.readBlock(data, count); }
  void readExactBlock(void* data, size_t count, const std::string& ctx) override { m_tape.readExactBlock(data, count, ctx); }
  void readFileMark(const std::string& ctx) override { m_tape.readFileMark(ctx); }
  bool isWriteProtected() override;
  bool isAtBOT() override { return m_tape.isAtBOT(); }
  bool isAtEOD() override { return m_tape.isAtEOD(); }
  bool isTapeBlank() override { return m_tape.isTapeBlank(); }
  lbpToUse getLbpToUse() override;
  bool hasTapeInPlace() override { return m_tape.isLoaded(); }

  // --- Drive-metadata and SCSI stubs (not tape-block operations) ---
  // FakeDrive is used in unit tests that never call these; they throw rather
  // than silently swallow to avoid masking unexpected call paths.
  std::map<std::string, uint32_t> getVolumeStats() override { return {}; }
  std::vector<uint16_t> getTapeAlertCodes() override { return {}; }
  std::vector<std::string> getTapeAlerts(const std::vector<uint16_t>&) override { return {}; }
  std::vector<std::string> getTapeAlertsCompact(const std::vector<uint16_t>&) override { return {}; }
  bool tapeAlertsCriticalForWrite(const std::vector<uint16_t>&) override { return false; }
  bool isEncryptionCapEnabled() override { return false; }
  void setEncryptionKey(const std::string&) override;
  void setDensityAndCompression(bool = true, unsigned char = 0) override;
  drive::LBPInfo getLBPInfo() override;
  void setLogicalBlockProtection(unsigned char, unsigned char, bool, bool) override;
  driveStatus getDriveStatus() override;
  void setSTBufferWrite(bool) override;
  cta::tape::SCSI::Structures::RAO::udsLimits getLimitUDS() override;
  void queryRAO(std::list<SCSI::Structures::RAO::blockLims>& files, int maxSupported) override;

  // Test helpers: direct access to in-memory tape state.
  void setTapeInPlace(bool tapeInPlace) { m_tape.setLoaded(tapeInPlace); }
  uint32_t getBlockIdPositioningCount() const { return m_blockIdPositioningCount; }

  // Returns a human-readable dump of the tape contents for test diagnostics.
  std::string contentToString() noexcept;
  void setFailurePoint(FailurePoint failurePoint, bool enabled = true);
  void clearFailurePoints();

private:
  void throwIfFailurePoint(FailurePoint failurePoint) const;
};

class FakeNonRAODrive : public FakeDrive {
public:
  FakeNonRAODrive();
  cta::tape::SCSI::Structures::RAO::udsLimits getLimitUDS() override;
};

}  // namespace cta::tape::drive
