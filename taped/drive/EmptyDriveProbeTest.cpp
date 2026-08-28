/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "EmptyDriveProbe.hpp"

#include "FakeDrive.hpp"

#include <gtest/gtest.h>
#include <stdexcept>

namespace cta::tape::daemon {
namespace {

struct ProbeObservation {
  bool released = false;
  bool cartridgePresent = false;
  drive::lbpToUse lbp = drive::lbpToUse::disabled;
};

// Observe the drive just before the probe releases it, without extending its lifetime.
class ObservedDrive final : public drive::FakeDrive {
public:
  explicit ObservedDrive(ProbeObservation& observation) : FakeDrive(5000, OnFlush, true), m_observation(observation) {
    enableCRC32CLogicalBlockProtectionReadWrite();
    // Any configuration reset, tape movement or alert access must fail the probe.
    for (const auto operation : {FailurePoint::ClearEncryptionKey,
                                 FailurePoint::DisableLogicalBlockProtection,
                                 FailurePoint::Rewind,
                                 FailurePoint::UnloadTape,
                                 FailurePoint::TapeAlertCodes,
                                 FailurePoint::TapeAlerts}) {
      setFailurePoint(operation);
    }
    // The constructor also makes waitUntilReady fail if it is called.
  }

  ~ObservedDrive() override {
    clearFailurePoints();
    m_observation.released = true;
    m_observation.cartridgePresent = hasTapeInPlace();
    m_observation.lbp = getLbpToUse();
  }

private:
  ProbeObservation& m_observation;
};

TEST(EmptyDriveProbeTest, OnlyProbesAndReleasesEmptyLoadedAndFailingDrives) {
  const common::dataStructures::DriveInfo info {"drive", "host", "library", "/dev/tape_T10D6116", "dummy"};
  for (const bool loaded : {false, true}) {
    for (const bool failure : {false, true}) {
      SCOPED_TRACE(loaded);
      SCOPED_TRACE(failure);
      ProbeObservation observation;
      System::fakeWrapper system;
      system.setupForVirtualDriveSLC6();
      auto* device = new ObservedDrive(observation);
      device->setTapeInPlace(loaded);
      device->setFailurePoint(drive::FakeDrive::FailurePoint::HasTapeInPlace, failure);
      const auto originalLbp = device->getLbpToUse();
      system.m_pathToDrive["/dev/nst0"] = device;

      const auto result = probeEmptyDrive(info, system);
      EXPECT_EQ(failure ? EmptyDriveProbeResult::Status::Failed :
                loaded  ? EmptyDriveProbeResult::Status::CartridgePresent :
                          EmptyDriveProbeResult::Status::Empty,
                result.status);
      EXPECT_EQ(!failure, result.errorMessage.empty());
      EXPECT_TRUE(observation.released);
      EXPECT_EQ(loaded, observation.cartridgePresent);
      EXPECT_EQ(originalLbp, observation.lbp);
      EXPECT_FALSE(system.m_pathToDrive.contains("/dev/nst0"));
    }
  }
}

TEST(EmptyDriveProbeTest, DiscoveryAndOpenFailuresReturnDiagnostics) {
  class FailingOpenWrapper final : public System::fakeWrapper {
  public:
    drive::DriveInterface* getDriveByPath(const std::string&) override {
      throw std::runtime_error("Drive opening failed");
    }
  };

  const common::dataStructures::DriveInfo info {"drive", "host", "library", "/dev/tape_T10D6116", "dummy"};
  for (const bool missingDevice : {false, true}) {
    FailingOpenWrapper system;
    system.setupForVirtualDriveSLC6();
    if (missingDevice) {
      system.m_stats.erase(info.devFilename);
    }
    // Inject drive acquisition failure rather than allowing the factory to create a default fake drive.
    const auto result = probeEmptyDrive(info, system);
    EXPECT_EQ(EmptyDriveProbeResult::Status::Failed, result.status);
    EXPECT_FALSE(result.errorMessage.empty());
  }
}

}  // namespace
}  // namespace cta::tape::daemon
