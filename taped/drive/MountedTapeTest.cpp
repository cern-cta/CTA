/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MountedTape.hpp"

#include "FakeDrive.hpp"
#include "catalogue/dummy/DummyCatalogue.hpp"
#include "mediachanger/RmcProxy.hpp"
#include "taped/session/TapeSessionTracker.hpp"

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
    if (body.find("Cleaner found no tape") != std::string_view::npos
        || body.find("Cleaner waiting for drive") != std::string_view::npos) {
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
  MountedTape::Outcome outcome;

  void SetUp() override {
    logger.tracker = &tracker;
    drive.info = driveInfo;
    volume.mountType = common::dataStructures::MountType::Retrieve;
    drive.setTapeInPlace(false);
    drive.enableCRC32CLogicalBlockProtectionReadWrite();
  }
};

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
      MountedTape tape(changer, volume, drive, catalogue, 0, [](auto) {}, outcome, lc, tracker);
      EXPECT_EQ(mountsBefore + 1, logger.mounts);
      EXPECT_EQ(cleanupsBefore, logger.cleanups);
      EXPECT_FALSE(outcome.driveReusable());
      EXPECT_NE(std::string::npos,
                logger.messages.find(mode == common::dataStructures::MountType::Retrieve ?
                                       "Tape mounted for read-only access" :
                                       "Tape mounted for read/write access"));
      EXPECT_NE(std::string::npos, logger.messages.find("MCMountTime"));
    }
    EXPECT_EQ(cleanupsBefore + 1, logger.cleanups);
    EXPECT_TRUE(outcome.driveReusable());
    EXPECT_EQ(drive::lbpToUse::disabled, drive.getLbpToUse());
  }
}

TEST_F(MountedTapeTest, ExplicitCleanupIsNotRepeatedByDestructor) {
  {
    MountedTape tape(changer, volume, drive, catalogue, 0, [](auto) {}, outcome, lc, tracker);
    EXPECT_TRUE(tape.cleanup().driveReusable());
    tape.cleanup();
  }
  EXPECT_EQ(1, logger.cleanups);
}

TEST_F(MountedTapeTest, FailedCleanupIsLoggedAndNeverRetried) {
  drive.setFailurePoint(drive::FakeDrive::FailurePoint::DisableLogicalBlockProtection);
  {
    MountedTape tape(changer, volume, drive, catalogue, 0, [](auto) {}, outcome, lc, tracker);
    EXPECT_FALSE(tape.cleanup().driveReusable());
    tape.cleanup();
  }
  EXPECT_EQ(1, logger.cleanups);
  ASSERT_TRUE(outcome.result);
  EXPECT_TRUE(outcome.result->configurationResetFailed);
  EXPECT_NE(std::string::npos, logger.messages.find("Mounted tape cleanup failed"));
}

TEST_F(MountedTapeTest, MountFailureCleansOnceAndRetainsMountOnlyTiming) {
  drive.info.rawLibrarySlot = "smc0";
  EXPECT_THROW((MountedTape {changer, volume, drive, catalogue, 0, [](auto) {}, outcome, lc, tracker}),
               cta::exception::Exception);
  EXPECT_EQ(1, logger.cleanups);
  EXPECT_TRUE(outcome.driveReusable());
  ASSERT_TRUE(logger.mountTimeBeforeCleanup);
  EXPECT_GT(*logger.mountTimeBeforeCleanup, 0);
  EXPECT_EQ(*logger.mountTimeBeforeCleanup, tracker.stats().setup.initialMountTime);
}

TEST_F(MountedTapeTest, CleanupFailureDoesNotReplaceMountFailure) {
  drive.setFailurePoint(drive::FakeDrive::FailurePoint::DisableLogicalBlockProtection);
  drive.info.rawLibrarySlot = "smc0";
  EXPECT_THROW((MountedTape {changer, volume, drive, catalogue, 0, [](auto) {}, outcome, lc, tracker}),
               cta::exception::Exception);
  EXPECT_EQ(1, logger.cleanups);
  EXPECT_FALSE(outcome.driveReusable());
  ASSERT_TRUE(outcome.result);
  EXPECT_TRUE(outcome.result->configurationResetFailed);
}

TEST_F(MountedTapeTest, CleanupFailureDoesNotInterruptUnwinding) {
  drive.setFailurePoint(drive::FakeDrive::FailurePoint::DisableLogicalBlockProtection);
  try {
    MountedTape tape(changer, volume, drive, catalogue, 0, [](auto) {}, outcome, lc, tracker);
    throw std::logic_error("transfer failed");
  } catch (const std::logic_error& ex) {
    EXPECT_STREQ("transfer failed", ex.what());
  }
  EXPECT_EQ(1, logger.cleanups);
  EXPECT_FALSE(outcome.driveReusable());
  ASSERT_TRUE(outcome.result);
}

TEST_F(MountedTapeTest, ReporterFailureDoesNotPreventPhysicalCleanup) {
  drive.setTapeInPlace(true);
  unsigned reports = 0;
  {
    MountedTape tape(
      changer,
      volume,
      drive,
      catalogue,
      0,
      [&](auto) {
        ++reports;
        throw std::runtime_error("report failed");
      },
      outcome,
      lc,
      tracker);
  }
  EXPECT_GT(reports, 0);
  EXPECT_TRUE(outcome.driveReusable());
  EXPECT_FALSE(drive.hasTapeInPlace());
  EXPECT_GT(tracker.failureStats().at(TapeSessionFailure::Reporting), 0);
}

}  // namespace
}  // namespace cta::tape::daemon
