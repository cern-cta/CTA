/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "VirtualDrive.hpp"

#include <set>
#include <string>

namespace cta::tape::drive {

/**
 * Fake drive used for unit testing.  Inherits all in-memory tape simulation
 * from VirtualDrive and layers configurable failure injection on top.
 */
class FakeDrive : public VirtualDrive {
public:
  enum FailureMoment { OnWrite, OnFlush };
  enum class FailurePoint { ClearEncryptionKey, Rewind, DisableLogicalBlockProtection, UnloadTape };

private:
  const enum FailureMoment m_failureMoment;
  std::set<FailurePoint> m_failurePoints;
  bool m_tapeOverflow = false;
  bool m_failToMount;

public:
  explicit FakeDrive(uint64_t capacity = std::numeric_limits<uint64_t>::max(),
                     enum FailureMoment failureMoment = OnWrite,
                     bool failOnMount = false) noexcept;
  explicit FakeDrive(bool failOnMount) noexcept;
  ~FakeDrive() override = default;

  // Failure-injecting overrides
  void writeBlock(const void* data, size_t count) override;
  void flush(void) override;
  void rewind(void) override;
  void disableLogicalBlockProtection() override;
  bool clearEncryptionKey() override;
  void unloadTape(void) override;
  void waitUntilReady(const uint32_t timeoutSecond) override;

  // Test-fixture overrides: return predictable non-zero values so unit tests
  // can assert on specific log output without depending on real drive hardware.
  std::map<std::string, uint64_t> getTapeWriteErrors() override;
  std::map<std::string, uint64_t> getTapeReadErrors() override;
  std::map<std::string, uint32_t> getTapeNonMediumErrors() override;
  std::map<std::string, float> getQualityStats() override;
  std::map<std::string, uint32_t> getDriveStats() override;
  std::string getDriveFirmwareVersion() override;
  deviceInfo getDeviceInfo() override;
  std::string getGenericSCSIPath() override;
  std::string getSerialNumber() override;

  // Test helpers
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
