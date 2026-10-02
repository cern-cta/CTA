/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "DriveInterface.hpp"

#include <chrono>
#include <limits>
#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace cta::tape::drive {

/// @brief In-memory tape records with configurable capacity, operation delays and injected failures.
///
/// Intended for tests; no hardware is accessed and some hardware-only operations are unsupported.
/// Callers must serialize access to tape contents and failure configuration.
class FakeDrive : public DriveInterface {
private:
  /// Stored record and remaining capacity after it; empty data represents a file mark.
  struct tapeBlock {
    std::string data;
    uint64_t remainingSpaceAfter;
  };

  std::vector<tapeBlock> m_tape;
  uint32_t m_currentPosition = 0;
  uint64_t m_tapeCapacity;
  int m_beginOfCompressStats = 0;

  /// Return the capacity remaining before the supplied record position, including full capacity at BOT.
  uint64_t getRemaingSpace(uint32_t currentPosition);

public:
  /// Select whether capacity exhaustion is reported while writing or deferred until flush.
  enum FailureMoment { OnWrite, OnFlush };

  /// Operations that support independently configured failures and delays.
  enum class FailurePoint {
    ClearEncryptionKey,
    Rewind,
    DisableLogicalBlockProtection,
    UnloadTape,
    HasTapeInPlace,
    TapeAlertCodes,
    TapeAlerts
  };

private:
  const enum FailureMoment m_failureMoment;
  std::set<FailurePoint> m_failurePoints;
  std::map<FailurePoint, std::chrono::microseconds> m_operationDelays;
  std::vector<uint16_t> m_tapeAlertCodes;
  bool m_tapeOverflow = false;
  bool m_failToMount;
  bool m_tapeInPlace = true;
  lbpToUse m_lbpToUse;

public:
  /// Render the current position and stored records for failure diagnostics.
  std::string contentToString() noexcept;

  /// Create an empty in-memory tape with configurable capacity, overflow timing and readiness failure.
  explicit FakeDrive(uint64_t capacity = std::numeric_limits<uint64_t>::max(),
                     enum FailureMoment failureMoment = OnWrite,
                     bool failOnMount = false) noexcept;

  /// Create an empty tape with unlimited capacity and an optional simulated readiness failure.
  explicit FakeDrive(bool failOnMount) noexcept;

  /// Release the stored in-memory tape records.
  ~FakeDrive() override = default;

  /// Count stored bytes since the last reset as both tape-read and tape-write bytes; host counters stay zero.
  compressionStats getCompression() final;

  /// Start subsequent compression accounting after the currently stored records.
  void clearCompressionStats() final;

  /// Return fixed synthetic metrics for tests that consume drive statistics.
  std::map<std::string, uint64_t> getTapeWriteErrors() final;

  /// Return fixed synthetic metrics for tests that consume drive statistics.
  std::map<std::string, uint64_t> getTapeReadErrors() final;

  /// Return fixed synthetic metrics for tests that consume drive statistics.
  std::map<std::string, uint32_t> getTapeNonMediumErrors() final;

  /// Return fixed synthetic metrics for tests that consume drive statistics.
  std::map<std::string, float> getQualityStats() final;

  /// Return fixed synthetic metrics for tests that consume drive statistics.
  std::map<std::string, uint32_t> getDriveStats() final;

  /// Return an empty map because volume statistics are not simulated.
  std::map<std::string, uint32_t> getVolumeStats() final;

  /// Return a fixed synthetic firmware revision.
  std::string getDriveFirmwareVersion() final;

  /// Return a fixed synthetic identity with protection-information support enabled.
  deviceInfo getDeviceInfo() final;

  /// Return a deliberately nonexistent generic SCSI path for the simulated drive.
  std::string getGenericSCSIPath() final;

  /// Throw cta::exception::NotImplementedException; this hardware operation is not simulated.
  std::string getSerialNumber() final;

  /// Count the positioning attempt and select an existing record; reject positions beyond stored data.
  void positionToLogicalObject(uint32_t blockId) final;

  /// Return the current record index with no buffered writes.
  positionInfo getPositionInfo() final;

  /// Encode the current record index as the wrap and report a zero longitudinal position.
  physicalPositionInfo getPhysicalPositionInfo() final;

  /// Return three fixed wrap boundaries for positioning tests.
  std::vector<endOfWrapPosition> getEndOfWrapPositions() final;

  /// Return configured TapeAlert codes after applying the configured delay or failure.
  std::vector<uint16_t> getTapeAlertCodes() final;

  /// Format synthetic alert messages after applying the configured delay or failure.
  std::vector<std::string> getTapeAlerts(const std::vector<uint16_t>&) final;

  /// Return an empty list; compact alert messages are not simulated.
  std::vector<std::string> getTapeAlertsCompact(const std::vector<uint16_t>&) final;

  /// Always return false; critical write alerts are not simulated.
  bool tapeAlertsCriticalForWrite(const std::vector<uint16_t>& codes) final;

  /// Throw cta::exception::NotImplementedException; this hardware operation is not simulated.
  void setDensityAndCompression(bool compression = true, unsigned char densityCode = 0) final;

  /// Select read-only CRC32C mode without computing checksums.
  void enableCRC32CLogicalBlockProtectionReadOnly() final;

  /// Select read/write CRC32C mode without computing checksums.
  void enableCRC32CLogicalBlockProtectionReadWrite() final;

  /// Select disabled protection after applying the configured delay or failure.
  void disableLogicalBlockProtection() final;

  /// Throw cta::exception::NotImplementedException; this hardware operation is not simulated.
  drive::LBPInfo getLBPInfo() final;

  /// Throw cta::exception::NotImplementedException; this hardware operation is not simulated.
  void setLogicalBlockProtection(const unsigned char method,
                                 unsigned char methodLength,
                                 const bool enableLPBforRead,
                                 const bool enableLBBforWrite) final;

  /// Throw cta::exception::Exception because key installation is not simulated.
  void setEncryptionKey(const std::string& encryption_key) final;

  /// Apply the configured delay or failure, then return false because encryption is unsupported.
  bool clearEncryptionKey() final;

  /// Always return false; encryption is not simulated.
  bool isEncryptionCapEnabled() final;

  /// Throw cta::exception::NotImplementedException; this hardware operation is not simulated.
  driveStatus getDriveStatus() final;

  /// Throw cta::exception::NotImplementedException; this hardware operation is not simulated.
  void setSTBufferWrite(bool bufWrite) final;

  /// Position on the last stored record; requires a nonempty simulated tape.
  void fastSpaceToEOM(void) final;

  /// Reset the record position to zero after applying the configured delay or failure.
  void rewind(void) final;

  /// Position on the last stored record; requires a nonempty simulated tape.
  void spaceToEOM(void) final;

  /// Move backwards across the requested number of file marks, towards the beginning of tape.
  void spaceFileMarksBackwards(size_t count) final;

  /// Move forwards across the requested number of file marks, towards the end of data.
  void spaceFileMarksForward(size_t count) final;

  /// Mark the cartridge absent after applying the configured delay or failure; retain stored records.
  void unloadTape(void) final;

  /// Raise an ENOSPC error for deferred capacity exhaustion; otherwise do nothing.
  void flush(void) final;

  /// Insert empty records without consuming capacity, truncating any subsequent tape contents.
  void writeSyncFileMarks(size_t count) final;

  /// Insert file marks synchronously; asynchronous buffering is not simulated.
  void writeImmediateFileMarks(size_t count) final;

  /// Store one record and truncate later records, reporting capacity exhaustion at the configured failure moment.
  void writeBlock(const void* data, size_t count) final;

  /// Copy the next stored record and advance; reject a buffer smaller than the record.
  ssize_t readBlock(void* data, size_t count) final;

  /// Copy and advance only if the next record size matches count; context is unused.
  void readExactBlock(void* data, size_t count, const std::string& context) final;

  /// Advance over an empty record or throw if the next record contains data; context is unused.
  void readFileMark(const std::string& context) final;

  /// Return immediately unless mount failure was configured; the timeout is ignored.
  void waitUntilReady(const uint32_t timeoutSecond) final;

  /// Always return false; write protection is not simulated.
  bool isWriteProtected() final;

  /// Check whether the tape is positioned at its beginning.
  bool isAtBOT() final;

  /// Check whether the current position is the last stored record.
  bool isAtEOD() final;

  /// Check whether the in-memory tape has no records, without changing position.
  bool isTapeBlank() final;

  /// Return the protection mode selected for the software read/write path.
  lbpToUse getLbpToUse() final;

  /// Return the configured cartridge-presence state after applying the configured delay or failure.
  bool hasTapeInPlace() final;

  /// Return fixed simulated limits of 30 segments and a maximum segment size of 30000.
  cta::tape::SCSI::Structures::RAO::udsLimits getLimitUDS() override;

  /// Reverse the supplied file list to simulate reordering; maxSupported is ignored.
  void queryRAO(std::list<SCSI::Structures::RAO::blockLims>& files, int maxSupported) final;

  /// Return the number of logical positioning attempts, including failed attempts.
  uint32_t getBlockIdPositioningCount() const;

  /// Set simulated cartridge presence independently of stored tape contents.
  void setTapeInPlace(bool tapeInPlace);

  /// Enable or disable failure injection for an operation without changing its configured delay.
  void setFailurePoint(FailurePoint failurePoint, bool enabled = true);

  /// Clear injected operation failures while retaining configured delays and capacity behavior.
  void clearFailurePoints();

  /// Replace the codes returned by subsequent TapeAlert queries.
  void setTapeAlertCodes(std::vector<uint16_t> codes);

  /// Delay the selected operation by the supplied duration, including when it is configured to fail.
  void setOperationDelay(FailurePoint operation, std::chrono::microseconds delay);

private:
  /// Apply the configured operation delay, then throw if failure injection is enabled.
  void throwIfFailurePoint(FailurePoint failurePoint) const;
  uint32_t m_blockIdPositioningCount = 0;
};

/// Fake drive that rejects RAO capability queries to exercise unsupported-drive handling.
class FakeNonRAODrive : public FakeDrive {
public:
  /// Create an empty fake tape whose RAO capability query reports unsupported operation.
  FakeNonRAODrive();

  /// Throw DriveDoesNotSupportRAOException to simulate a drive without enterprise RAO.
  cta::tape::SCSI::Structures::RAO::udsLimits getLimitUDS() final;
};

}  // namespace cta::tape::drive
