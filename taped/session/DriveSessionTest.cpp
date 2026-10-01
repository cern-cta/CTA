/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveSession.hpp"

#include "catalogue/dummy/DummyCatalogue.hpp"
#include "common/log/StringLogger.hpp"
#include "taped/SchedulerContext.hpp"
#include "taped/SchedulerTestUtils.hpp"
#include "taped/drive/FakeDrive.hpp"

#include <functional>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <stdexcept>

namespace cta::tape::daemon {
namespace {
using namespace common::dataStructures;

class SessionDriveState : public catalogue::DummyDriveStateCatalogue {
public:
  bool missing = false;
  std::function<void()> onRead;
  std::function<void(DriveStatus)> onReport;
  std::vector<DriveStatus> reports;

  std::optional<TapeDrive> getTapeDrive(const std::string& name) const override {
    if (onRead) {
      onRead();
    }
    return missing ? std::nullopt : DummyDriveStateCatalogue::getTapeDrive(name);
  }

  bool updateTapeDriveStatus(const TapeDrive& drive) override {
    reports.push_back(drive.driveStatus);
    if (onReport) {
      onReport(drive.driveStatus);
    }
    return DummyDriveStateCatalogue::updateTapeDriveStatus(drive);
  }
};

class SessionCatalogue : public catalogue::DummyCatalogue {
public:
  SessionCatalogue() { m_driveState = std::make_unique<SessionDriveState>(); }

  SessionDriveState& driveState() { return static_cast<SessionDriveState&>(*m_driveState); }
};
}  // namespace

class DriveSessionTest : public testing::Test {
protected:
  TapedConfig config;
  log::StringLogger logger {"host", "DriveSessionTest", log::DEBUG};
  std::unique_ptr<catalogue::Catalogue> catalogue = std::make_unique<SessionCatalogue>();
  std::unique_ptr<SchedulerDatabase> db;
  std::unique_ptr<Scheduler> scheduler;
  std::unique_ptr<SchedulerContext> context;
  System::fakeWrapper system;
  std::stop_source stop;
  std::unique_ptr<DriveSession> session;

  SessionDriveState& driveState() { return static_cast<SessionCatalogue&>(*catalogue).driveState(); }

  void SetUp() override {
    config.drive.name = "drive";
    config.drive.logical_library_name = "library";
    config.drive.device = "/dev/tape_T10D6116";
    config.drive.control_path = "dummy";
    config.mounts.tape_load_timeout_secs = 0;
    db = testingUtils::createSchedulerDatabase(catalogue);
    scheduler = std::make_unique<Scheduler>(*catalogue, *db, "test");
    context = std::make_unique<SchedulerContext>(config, logger, *scheduler);
    system.setupForVirtualDriveSLC6();
    installEmptyDrive();
    TapeDrive drive;
    drive.driveName = "drive";
    drive.driveStatus = DriveStatus::Down;
    drive.mountType = MountType::NoMount;
    driveState().updateTapeDriveStatus(drive);
    requestUp(true);
    driveState().reports.clear();
    session = DriveSession::create(config, logger, *context, system);
  }

  void TearDown() override {
    driveState().onRead = {};
    driveState().onReport = {};
    session.reset();
  }

  void installEmptyDrive() {
    auto* drive = new drive::FakeDrive(5000, drive::FakeDrive::OnFlush);
    drive->setTapeInPlace(false);
    system.m_pathToDrive["/dev/nst0"] = drive;
  }

  void requestUp(bool up, std::optional<std::string> reason = std::nullopt) {
    DesiredDriveState desired;
    desired.up = up;
    desired.reason = reason;
    driveState().setDesiredTapeDriveState("drive", desired);
  }

  TapeDrive reported() { return driveState().getTapeDrive("drive").value(); }

  bool canUseHardware() { return session->m_hardwareOwnership.canUseHardware(); }

  bool clean(const std::optional<std::string>& vid = std::nullopt) { return session->cleanDrive(vid); }

  void markUnsafe() { session->m_hardwareOwnership.markUnsafe(); }
};

TEST_F(DriveSessionTest, ConstructionDefersPreparationUntilRun) {
  EXPECT_TRUE(driveState().reports.empty());
  EXPECT_FALSE(canUseHardware());
  EXPECT_TRUE(system.m_pathToDrive.contains("/dev/nst0"));
  // End after preparation, before attempting to schedule a transfer.
  driveState().onReport = [&](DriveStatus status) {
    if (status == DriveStatus::Up) {
      stop.request_stop();
    }
  };
  session->run(stop.get_token());
  EXPECT_FALSE(system.m_pathToDrive.contains("/dev/nst0"));
  EXPECT_THAT(driveState().reports, testing::Contains(DriveStatus::CleaningUp));
  EXPECT_THAT(driveState().reports, testing::Contains(DriveStatus::Up));
  EXPECT_EQ(DriveStatus::Down, reported().driveStatus);
  EXPECT_TRUE(reported().desiredUp);
  EXPECT_FALSE(canUseHardware());
  EXPECT_TRUE(session->isLive());
}

TEST_F(DriveSessionTest, RequestedDownSkipsHardwareAndPreservesOperatorReason) {
  requestUp(false, "Maintenance");
  session->run(stop.get_token());
  EXPECT_TRUE(system.m_pathToDrive.contains("/dev/nst0"));
  EXPECT_EQ("Maintenance", reported().reasonUpDown);
  EXPECT_FALSE(canUseHardware());
}

TEST_F(DriveSessionTest, StopBeforeRunSkipsPreparation) {
  stop.request_stop();
  session->run(stop.get_token());
  EXPECT_TRUE(system.m_pathToDrive.contains("/dev/nst0"));
  EXPECT_THAT(driveState().reports, testing::ElementsAre(DriveStatus::Down));
}

TEST_F(DriveSessionTest, DownDuringPreparationPreventsUpPublication) {
  driveState().onReport = [&](DriveStatus status) {
    if (status == DriveStatus::CleaningUp) {
      requestUp(false, "Maintenance");
    }
  };
  session->run(stop.get_token());
  EXPECT_FALSE(system.m_pathToDrive.contains("/dev/nst0"));
  EXPECT_THAT(driveState().reports, testing::Not(testing::Contains(DriveStatus::Up)));
  EXPECT_EQ("Maintenance", reported().reasonUpDown);
  EXPECT_EQ(DriveStatus::Down, reported().driveStatus);
}

TEST_F(DriveSessionTest, FailedPreparationRequestsDownAndReleasesOwnership) {
  // A device discovery failure exercises the real cleaner's failure handling.
  system.m_stats.erase(config.drive.device);
  session->run(stop.get_token());
  EXPECT_FALSE(reported().desiredUp);
  ASSERT_TRUE(reported().reasonUpDown);
  EXPECT_THAT(*reported().reasonUpDown, testing::HasSubstr("Drive cleanup failed"));
  EXPECT_EQ(DriveStatus::Down, reported().driveStatus);
  EXPECT_FALSE(canUseHardware());
  EXPECT_TRUE(session->isLive());
}

TEST_F(DriveSessionTest, MissingDrivePropagatesWithoutHardwareAccess) {
  driveState().missing = true;
  EXPECT_THROW(session->run(stop.get_token()), Scheduler::NoSuchDrive);
  EXPECT_TRUE(system.m_pathToDrive.contains("/dev/nst0"));
  EXPECT_TRUE(driveState().reports.empty());
  EXPECT_TRUE(session->isLive());
}

TEST_F(DriveSessionTest, PreparationPublicationFailurePreservesExceptionAndClearsTracker) {
  driveState().onReport = [](DriveStatus status) {
    if (status == DriveStatus::CleaningUp) {
      throw std::runtime_error("publication failed");
    }
  };
  EXPECT_THROW(session->run(stop.get_token()), std::runtime_error);
  EXPECT_FALSE(canUseHardware());
  EXPECT_TRUE(system.m_pathToDrive.contains("/dev/nst0"));
  EXPECT_TRUE(session->isLive());
  EXPECT_FALSE(reported().desiredUp);
}

TEST_F(DriveSessionTest, RecoveryCleansEvenAfterOperatorRequestsDown) {
  ASSERT_TRUE(clean());
  ASSERT_TRUE(canUseHardware());
  installEmptyDrive();
  requestUp(false, "Maintenance");
  EXPECT_FALSE(clean("V00001"));
  EXPECT_FALSE(system.m_pathToDrive.contains("/dev/nst0"));
  EXPECT_TRUE(canUseHardware());
  session.reset();
  EXPECT_EQ(DriveStatus::Down, reported().driveStatus);
  EXPECT_EQ("Maintenance", reported().reasonUpDown);
}

TEST_F(DriveSessionTest, DestructorReleasesAndPreservesPendingUpRequest) {
  ASSERT_TRUE(clean());
  requestUp(true, "Pending operator request");
  session.reset();
  EXPECT_EQ(DriveStatus::Down, reported().driveStatus);
  EXPECT_TRUE(reported().desiredUp);
  EXPECT_EQ("Pending operator request", reported().reasonUpDown);
}

TEST_F(DriveSessionTest, DestructorContainsPublicationFailure) {
  ASSERT_TRUE(clean());
  driveState().onReport = [](DriveStatus) { throw std::runtime_error("unavailable"); };
  EXPECT_NO_THROW(session.reset());
  EXPECT_THAT(logger.getLog(), testing::HasSubstr("Failed to release drive session or publish Down"));
}

TEST_F(DriveSessionTest, UnsafeOwnershipPreventsTerminalPublication) {
  ASSERT_TRUE(clean());
  markUnsafe();
  driveState().reports.clear();
  session.reset();
  EXPECT_TRUE(driveState().reports.empty());
}

}  // namespace cta::tape::daemon
