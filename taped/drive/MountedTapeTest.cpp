/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MountedTape.hpp"

#include "DriveGeneric.hpp"
#include "FakeDrive.hpp"
#include "catalogue/dummy/DummyCatalogue.hpp"
#include "mediachanger/RmcProxy.hpp"
#include "taped/session/TapeSessionTracker.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <stdexcept>
#include <type_traits>

namespace cta::tape::daemon {
namespace {

static_assert(!std::is_copy_constructible_v<MountedTape>);
static_assert(!std::is_move_constructible_v<MountedTape>);
static_assert(std::is_nothrow_destructible_v<MountedTape>);

// Observe the concrete robot and cleaner operations, including timing before cleanup.
class MountLogger : public log::Logger {
public:
  MountLogger() : Logger("host", "MountedTapeTest", log::DEBUG) {}

  void refresh() override {}

  unsigned mounts = 0;
  unsigned cleanups = 0;
  std::string messages;
  TapeSessionTracker* tracker = nullptr;
  std::optional<double> mountTimeBeforeCleanup;

protected:
  void writeMsgToUnderlyingLoggingSystem(std::string_view, std::string_view body) override {
    messages += body;
    if (body.find("Dummy mount for") != std::string_view::npos) {
      ++mounts;
    }
    // Each cleanup checks readiness, including when the drive is empty.
    if (body.find("Cleaner checking media readiness") != std::string_view::npos) {
      ++cleanups;
      mountTimeBeforeCleanup = tracker->stats().setup.initialMountTime;
    }
  }
};

class MountedTapeTest : public testing::Test {
protected:
  MountLogger logger;
  log::LogContext lc {logger};
  catalogue::DummyCatalogue catalogue;
  mediachanger::RmcProxy proxy {"localhost", 0, 1, 1};
  mediachanger::MediaChangerFacade changer {proxy, logger};
  TapeSessionTracker tracker;
  common::dataStructures::DriveInfo driveInfo {"drive", "host", "library", "device", "dummy"};
  drive::FakeDrive drive {5000, drive::FakeDrive::OnFlush};
  VolumeInfo volume {};
  std::optional<DriveCleaner::CleanupResult> outcome;

  void SetUp() override {
    logger.tracker = &tracker;
    drive.info = driveInfo;
    volume.mountType = common::dataStructures::MountType::Retrieve;
    drive.setTapeInPlace(false);
    drive.enableCRC32CLogicalBlockProtectionReadWrite();
  }
};

// Observe the deadline at the drive boundary without sleeping or emulating kernel behavior.
class ReadinessDrive : public drive::DriveLTO {
public:
  using DriveLTO::DriveLTO;
  MOCK_METHOD(void, waitUntilReady, (std::chrono::steady_clock::time_point), (override));

  bool clearEncryptionKey() override { return false; }

  void disableLogicalBlockProtection() override {}

  void unloadTape() override {}
};

TEST_F(MountedTapeTest, CleanupUsesUnloadTimeoutForReadiness) {
  using Clock = std::chrono::steady_clock;
  using namespace std::chrono_literals;
  using testing::_;

  for (const std::string reason : {"Media is absent", "Drive readiness timed out"}) {
    SCOPED_TRACE(reason);
    testing::StrictMock<System::mockWrapper> wrapper;
    EXPECT_CALL(wrapper, open(_, _)).WillOnce(testing::Return(7));
    EXPECT_CALL(wrapper, close(7)).WillOnce(testing::Return(0));
    testing::StrictMock<ReadinessDrive> hardware(SCSI::DeviceInfo {}, wrapper);
    hardware.info = driveInfo;
    MountedTape tape(changer, volume, hardware, catalogue, 30, outcome, lc, tracker);
    const auto beforeCleanup = Clock::now();
    EXPECT_CALL(hardware, waitUntilReady(_)).WillOnce([&](auto deadline) {
      EXPECT_GE(deadline, beforeCleanup + 30s);
      EXPECT_LE(deadline, Clock::now() + 30s);
      throw drive::MediaNotReady(reason);
    });
    EXPECT_TRUE(tape.cleanup().driveReusable());
    EXPECT_THAT(logger.messages, testing::HasSubstr(reason));
    EXPECT_EQ(0, tracker.stats().cleanup.labelReadTime);
  }
}

TEST_F(MountedTapeTest, MountsBothAccessModesAndCleansOnScopeExit) {
  for (const auto mode : {common::dataStructures::MountType::Retrieve,
                          common::dataStructures::MountType::ArchiveForUser,
                          common::dataStructures::MountType::ArchiveForRepack}) {
    volume.mountType = mode;
    volume.vid = "TAPE01";
    logger.messages.clear();
    const auto mountsBefore = logger.mounts;
    const auto cleanupsBefore = logger.cleanups;
    {
      MountedTape tape(changer, volume, drive, catalogue, 30, outcome, lc, tracker);
      EXPECT_EQ(mountsBefore + 1, logger.mounts);
      EXPECT_EQ(cleanupsBefore, logger.cleanups);
      EXPECT_FALSE(outcome);
      EXPECT_NE(std::string::npos,
                logger.messages.find(mode == common::dataStructures::MountType::Retrieve ?
                                       "Tape mounted for read-only access" :
                                       "Tape mounted for read/write access"));
      EXPECT_NE(std::string::npos, logger.messages.find("MCMountTime"));
    }
    EXPECT_EQ(cleanupsBefore + 1, logger.cleanups);
    ASSERT_TRUE(outcome);
    EXPECT_TRUE(outcome->driveReusable());
    EXPECT_EQ(drive::lbpToUse::disabled, drive.getLbpToUse());
  }
}

TEST_F(MountedTapeTest, ExplicitCleanupIsNotRepeatedByDestructor) {
  {
    MountedTape tape(changer, volume, drive, catalogue, 30, outcome, lc, tracker);
    EXPECT_TRUE(tape.cleanup().driveReusable());
    tape.cleanup();
  }
  EXPECT_EQ(1, logger.cleanups);
}

TEST_F(MountedTapeTest, FailedCleanupIsLoggedAndNeverRetried) {
  drive.setFailurePoint(drive::FakeDrive::FailurePoint::DisableLogicalBlockProtection);
  {
    MountedTape tape(changer, volume, drive, catalogue, 30, outcome, lc, tracker);
    EXPECT_FALSE(tape.cleanup().driveReusable());

    // A retry would now succeed; cleanup must preserve the original failure.
    drive.clearFailurePoints();
    EXPECT_TRUE(tape.cleanup().configurationResetFailed);
    EXPECT_FALSE(tape.cleanup().driveReusable());
  }
  EXPECT_EQ(1, logger.cleanups);
  ASSERT_TRUE(outcome);
  EXPECT_TRUE(outcome->configurationResetFailed);
  EXPECT_FALSE(outcome->driveReusable());
  EXPECT_NE(std::string::npos, logger.messages.find("Mounted tape cleanup failed"));
}

TEST_F(MountedTapeTest, MountFailureCleansOnceAndRetainsMountOnlyTiming) {
  drive.info.rawLibrarySlot = "smc0";
  EXPECT_THROW((MountedTape {changer, volume, drive, catalogue, 30, outcome, lc, tracker}), cta::exception::Exception);
  EXPECT_EQ(1, logger.cleanups);
  ASSERT_TRUE(outcome);
  EXPECT_FALSE(outcome->driveReusable());
  EXPECT_TRUE(outcome->ejectFailed);
  ASSERT_TRUE(logger.mountTimeBeforeCleanup);
  EXPECT_GT(*logger.mountTimeBeforeCleanup, 0);
  EXPECT_EQ(*logger.mountTimeBeforeCleanup, tracker.stats().setup.initialMountTime);
}

TEST_F(MountedTapeTest, CleanupFailureDoesNotReplaceMountFailure) {
  drive.setFailurePoint(drive::FakeDrive::FailurePoint::DisableLogicalBlockProtection);
  drive.info.rawLibrarySlot = "smc0";
  EXPECT_THROW((MountedTape {changer, volume, drive, catalogue, 30, outcome, lc, tracker}), cta::exception::Exception);
  EXPECT_EQ(1, logger.cleanups);
  EXPECT_FALSE(outcome && outcome->driveReusable());
  ASSERT_TRUE(outcome);
  EXPECT_TRUE(outcome->configurationResetFailed);
}

TEST_F(MountedTapeTest, CleanupFailureDoesNotInterruptUnwinding) {
  drive.setFailurePoint(drive::FakeDrive::FailurePoint::DisableLogicalBlockProtection);
  try {
    MountedTape tape(changer, volume, drive, catalogue, 30, outcome, lc, tracker);
    throw std::logic_error("transfer failed");
  } catch (const std::logic_error& ex) {
    EXPECT_STREQ("transfer failed", ex.what());
  }
  EXPECT_EQ(1, logger.cleanups);
  EXPECT_FALSE(outcome && outcome->driveReusable());
  ASSERT_TRUE(outcome);
}

TEST_F(MountedTapeTest, ReporterFailureDoesNotPreventPhysicalCleanup) {
  drive.setTapeInPlace(true);
  unsigned reports = 0;
  TapeSessionTracker reportingTracker([&](auto) {
    ++reports;
    throw std::runtime_error("report failed");
  });
  logger.tracker = &reportingTracker;
  { MountedTape tape(changer, volume, drive, catalogue, 30, outcome, lc, reportingTracker); }
  EXPECT_GT(reports, 0);
  ASSERT_TRUE(outcome);
  EXPECT_TRUE(outcome->driveReusable());
  EXPECT_FALSE(drive.hasTapeInPlace());
  EXPECT_GT(reportingTracker.outcomeSnapshot().failures[static_cast<size_t>(TapeSessionFailure::Reporting)], 0);
}

}  // namespace
}  // namespace cta::tape::daemon
