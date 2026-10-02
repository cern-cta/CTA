/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveSession.hpp"

#include "catalogue/dummy/DummyCatalogue.hpp"
#include "common/log/StringLogger.hpp"
#include "taped/SchedulerContext.hpp"
#include "taped/drive/FakeDrive.hpp"

#include <functional>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <stdexcept>

#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBTestFactory.hpp"
#else
#include "objectstore/BackendVFS.hpp"
#include "scheduler/OStoreDB/OStoreDBFactory.hpp"
#endif

namespace cta::tape::daemon {
namespace {
using namespace common::dataStructures;

class SessionDriveState : public catalogue::DummyDriveStateCatalogue {
public:
  bool missing = false;
  std::function<void()> onRead;
  std::function<void(DriveStatus)> onReport;
  std::function<void(const DesiredDriveState&)> onDesired;
  std::vector<DriveStatus> reports;

  std::optional<TapeDrive> getTapeDrive(const std::string& name) const override {
    if (onRead) {
      onRead();
    }
    return missing ? std::nullopt : DummyDriveStateCatalogue::getTapeDrive(name);
  }

  void setDesiredTapeDriveState(const std::string& name, const DesiredDriveState& state) override {
    if (onDesired) {
      onDesired(state);
    }
    DummyDriveStateCatalogue::setDesiredTapeDriveState(name, state);
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
#ifdef CTA_PGSCHED
    db = RelationalDBTestFactory().create(catalogue);
#else
    db = OStoreDBFactory<objectstore::BackendVFS>().create(catalogue);
#endif
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
    driveState().onDesired = {};
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

  bool canUseHardware() { return session->m_driveReservation.canUseHardware(); }

  bool clean(const std::optional<std::string>& vid = std::nullopt) { return session->cleanDrive(vid); }

  std::shared_ptr<const TapeSessionTracker> activeTracker() { return std::atomic_load(&session->m_activeTracker); }

  void publish(const std::shared_ptr<TapeSessionTracker>& tracker) {
    std::atomic_store<const TapeSessionTracker>(&session->m_activeTracker, tracker);
  }

  void checkCleanupStates() {
    using namespace std::chrono_literals;
    using enum cta::tape::session::TapeSessionState;
    const auto tracker = std::const_pointer_cast<TapeSessionTracker>(activeTracker());
    ASSERT_TRUE(tracker);
    EXPECT_EQ(Preparing, tracker->livenessSnapshot().state);
    EXPECT_TRUE(session->isLive());
    const auto now = TapeSessionTracker::Clock::now();
    tracker->reportState(Unloading, now - std::chrono::seconds(config.mounts.tape_unload_timeout_secs) - 1s);
    EXPECT_FALSE(session->isLive());
    tracker->reportState(Unmounting, now - std::chrono::seconds(config.mounts.unmount_timeout_secs) - 1s);
    EXPECT_FALSE(session->isLive());
    // Cleanup uses the same phase limits as transfers.
    tracker->reportState(Finalizing, now - 24h);
    EXPECT_FALSE(session->isLive());
    tracker->reportState(Preparing, now - 24h);
    EXPECT_FALSE(session->isLive());
  }

  bool recover() {
    session->m_driveReservation.acquire();
    return session->cleanDrive("V00001");
  }

  void markUnsafe() { session->m_driveReservation.markUnsafe(); }
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

TEST_F(DriveSessionTest, CleanupDesiredStatePublicationFailurePropagatesUntilOwnershipIsReleased) {
  auto* drive = static_cast<drive::FakeDrive*>(system.m_pathToDrive.at("/dev/nst0"));
  drive->setFailurePoint(drive::FakeDrive::FailurePoint::ClearEncryptionKey);
  unsigned int desiredAttempts = 0;
  driveState().onDesired = [&](const DesiredDriveState& desired) {
    ++desiredAttempts;
    EXPECT_FALSE(desired.up);
    EXPECT_TRUE(canUseHardware());
    throw std::runtime_error("desired publication failed");
  };

  EXPECT_THROW(session->run(stop.get_token()), std::runtime_error);
  EXPECT_GT(desiredAttempts, 0);
  EXPECT_TRUE(canUseHardware());
  EXPECT_TRUE(reported().desiredUp);
  EXPECT_EQ(DriveStatus::CleaningUp, reported().driveStatus);
  EXPECT_FALSE(activeTracker());

  // A failed request does not acknowledge release; teardown publishes Down after releasing ownership.
  session.reset();
  EXPECT_EQ(DriveStatus::Down, reported().driveStatus);
  EXPECT_TRUE(reported().desiredUp);
}

TEST_F(DriveSessionTest, CleanupReportedDownFailurePropagatesAfterOwnershipIsReleased) {
  auto* drive = static_cast<drive::FakeDrive*>(system.m_pathToDrive.at("/dev/nst0"));
  drive->setFailurePoint(drive::FakeDrive::FailurePoint::ClearEncryptionKey);
  unsigned int downAttempts = 0;
  driveState().onReport = [&](DriveStatus status) {
    if (status == DriveStatus::Down) {
      ++downAttempts;
      EXPECT_FALSE(canUseHardware());
      throw std::runtime_error("reported publication failed");
    }
  };

  EXPECT_THROW(session->run(stop.get_token()), std::runtime_error);
  EXPECT_EQ(1, downAttempts);
  EXPECT_FALSE(canUseHardware());
  EXPECT_FALSE(reported().desiredUp);
  EXPECT_EQ(DriveStatus::CleaningUp, reported().driveStatus);
  ASSERT_TRUE(reported().reasonUpDown);
  EXPECT_THAT(*reported().reasonUpDown, testing::HasSubstr("Failed to clear encryption key"));
  EXPECT_FALSE(activeTracker());
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

TEST_F(DriveSessionTest, PreparationIsVisibleAndClearedOnNormalExit) {
  requestUp(false);
  EXPECT_FALSE(activeTracker());
  unsigned int observed = 0;
  driveState().onRead = [&] {
    if (activeTracker()) {
      ++observed;
      checkCleanupStates();
    }
  };
  // Desired Down returns normally before hardware access, then releases and reports Down.
  session->run(std::stop_token {});
  EXPECT_EQ(1, observed);
  EXPECT_FALSE(activeTracker());
  EXPECT_TRUE(session->isLive());
  EXPECT_FALSE(canUseHardware());
}

TEST_F(DriveSessionTest, PreparationExceptionClearsTracker) {
  driveState().onRead = [&] {
    if (activeTracker()) {
      checkCleanupStates();
      throw std::runtime_error("catalogue unavailable");
    }
  };
  EXPECT_THROW(session->run(std::stop_token {}), std::runtime_error);
  EXPECT_FALSE(activeTracker());
  EXPECT_TRUE(session->isLive());
}

TEST_F(DriveSessionTest, RecoveryExceptionClearsTrackerAndRetainsOwnership) {
  driveState().onRead = [&] {
    checkCleanupStates();
    throw std::runtime_error("catalogue unavailable");
  };
  EXPECT_THROW(recover(), std::runtime_error);
  EXPECT_FALSE(activeTracker());
  EXPECT_TRUE(session->isLive());
  EXPECT_TRUE(canUseHardware());
}

TEST_F(DriveSessionTest, TransferStateTimeoutsAndIdleLivenessRemainUnchanged) {
  using namespace std::chrono_literals;
  using enum cta::tape::session::TapeSessionState;
  auto tracker = std::make_shared<TapeSessionTracker>();
  publish(tracker);
  const auto now = TapeSessionTracker::Clock::now();
  tracker->beginTapeSession(now);
  tracker->reportState(Unloading, now - std::chrono::seconds(config.mounts.tape_unload_timeout_secs) - 1s);
  EXPECT_FALSE(session->isLive());
  tracker->reportState(Loading, now);
  tracker->reportState(Unloading, now);
  EXPECT_TRUE(session->isLive());
  tracker->reportState(Finalizing, now - 24h);
  EXPECT_FALSE(session->isLive());
  publish(nullptr);
  EXPECT_TRUE(session->isLive());
}

TEST_F(DriveSessionTest, ConfiguredPhaseTimeoutsDoNotResetOnRepeatedStateReports) {
  using namespace std::chrono_literals;
  using enum cta::tape::session::TapeSessionState;
  config.mounts.preparing_timeout_secs = 10;
  config.mounts.finalizing_timeout_secs = 20;
  auto tracker = std::make_shared<TapeSessionTracker>();
  publish(tracker);
  const auto now = TapeSessionTracker::Clock::now();
  for (const auto state : {Preparing, Finalizing}) {
    const auto limit = std::chrono::seconds(state == Preparing ? 10 : 20);
    tracker->reportState(Finished, now);
    tracker->reportState(state, now - limit + 5s);
    EXPECT_TRUE(session->isLive());
    tracker->reportState(Finished, now);
    tracker->reportState(state, now - limit - 1s);
    EXPECT_FALSE(session->isLive());
    tracker->reportState(state, now);
    EXPECT_FALSE(session->isLive());
  }
}

TEST_F(DriveSessionTest, EachTransferPhaseUsesItsConfiguredTimeout) {
  using namespace std::chrono_literals;
  using enum cta::tape::session::TapeSessionState;
  config.mounts.mount_timeout_secs = 11;
  config.mounts.tape_load_timeout_secs = 12;
  config.mounts.tape_unload_timeout_secs = 13;
  config.mounts.unmount_timeout_secs = 14;
  config.transfers.no_block_move_timeout_secs = 15;
  config.transfers.retrieve.drain_to_disk_timeout_secs = 16;
  const std::pair<cta::tape::session::TapeSessionState, unsigned int> cases[] {
    {Mounting,       11},
    {Loading,        12},
    {Unloading,      13},
    {Unmounting,     14},
    {Transferring,   15},
    {DrainingToDisk, 16}
  };
  const auto now = TapeSessionTracker::Clock::now();
  for (const auto& [state, seconds] : cases) {
    SCOPED_TRACE(static_cast<int>(state));
    for (const bool expired : {false, true}) {
      auto tracker = std::make_shared<TapeSessionTracker>();
      const auto enteredAt = now - std::chrono::seconds(seconds) + (expired ? -1s : 5s);
      tracker->beginTapeSession(enteredAt);
      tracker->reportState(state, enteredAt);
      publish(tracker);
      EXPECT_EQ(!expired, session->isLive());
      // Neither repeated state publication nor statistics reporting is block movement.
      tracker->reportState(state, now);
      tracker->updateTapeTransferStats({.dataVolume = 100});
      EXPECT_EQ(!expired, session->isLive());
    }
  }
  auto tracker = std::make_shared<TapeSessionTracker>();
  publish(tracker);
  EXPECT_TRUE(session->isLive());
  tracker->reportState(Finished, now - 24h);
  EXPECT_TRUE(session->isLive());
}

TEST_F(DriveSessionTest, BlockMovementRefreshesTransferLivenessAndNewSessionResetsIt) {
  using namespace std::chrono_literals;
  using enum cta::tape::session::TapeSessionState;
  config.transfers.no_block_move_timeout_secs = 10;
  config.mounts.tape_unload_timeout_secs = 10;
  const auto now = TapeSessionTracker::Clock::now();
  auto tracker = std::make_shared<TapeSessionTracker>();
  publish(tracker);
  tracker->beginTapeSession(now - 30s);
  tracker->notifyBlockMovement(1, now - 25s);
  tracker->reportState(Transferring, now - 20s);
  EXPECT_FALSE(session->isLive());
  tracker->notifyBlockMovement(1, now);
  EXPECT_TRUE(session->isLive());
  tracker->reportState(Unloading, now - 20s);
  EXPECT_FALSE(session->isLive());
  // Starting a new session clears the previous session's block-movement timestamp.
  tracker->beginTapeSession(now - 20s);
  tracker->reportState(Transferring, now - 20s);
  EXPECT_FALSE(session->isLive());
  tracker->beginTapeSession(now);
  tracker->reportState(Transferring, now);
  EXPECT_TRUE(session->isLive());
}

}  // namespace cta::tape::daemon
