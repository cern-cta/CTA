/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveSession.hpp"
#include "catalogue/dummy/DummyCatalogue.hpp"
#include "common/log/StringLogger.hpp"
#include "taped/SchedulerContext.hpp"
#include "taped/SchedulerTestUtils.hpp"

#include <functional>
#include <gtest/gtest.h>
#include <stdexcept>

namespace cta::tape::daemon {
namespace {
class ObservedDriveState final : public catalogue::DummyDriveStateCatalogue {
public:
  std::function<void()> onRead;

  std::optional<common::dataStructures::TapeDrive> getTapeDrive(const std::string& name) const override {
    if (onRead) {
      onRead();
    }
    return DummyDriveStateCatalogue::getTapeDrive(name);
  }
};

class CleanupCatalogue final : public catalogue::DummyCatalogue {
public:
  CleanupCatalogue() { m_driveState = std::make_unique<ObservedDriveState>(); }

  ObservedDriveState& driveState() { return static_cast<ObservedDriveState&>(*m_driveState); }
};
}  // namespace

class DriveSessionLivenessTest : public testing::Test {
protected:
  TapedConfig config;
  log::StringLogger logger {"host", "cleanup-liveness", log::DEBUG};
  std::unique_ptr<catalogue::Catalogue> catalogue = std::make_unique<CleanupCatalogue>();
  std::unique_ptr<SchedulerDatabase> db;
  std::unique_ptr<Scheduler> scheduler;
  std::unique_ptr<SchedulerContext> schedulerContext;
  std::unique_ptr<DriveSession> session;

  ObservedDriveState& driveState() { return static_cast<CleanupCatalogue&>(*catalogue).driveState(); }

  void SetUp() override {
    config.drive.name = "drive";
    db = testingUtils::createSchedulerDatabase(catalogue);
    scheduler = std::make_unique<Scheduler>(*catalogue, *db, "test");
    schedulerContext = std::make_unique<SchedulerContext>(config, logger, *scheduler);
    session = DriveSession::create(config, logger, *schedulerContext);
  }

  void TearDown() override {
    driveState().onRead = {};
    session.reset();
  }

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
    session->m_hardwareOwnership.acquire();
    return session->cleanDrive("V00001");
  }

  bool canUseHardware() { return session->m_hardwareOwnership.canUseHardware(); }
};

TEST_F(DriveSessionLivenessTest, PreparationIsVisibleAndClearedOnNormalExit) {
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

TEST_F(DriveSessionLivenessTest, PreparationExceptionClearsTracker) {
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

TEST_F(DriveSessionLivenessTest, RecoveryExceptionClearsTrackerAndRetainsOwnership) {
  driveState().onRead = [&] {
    checkCleanupStates();
    throw std::runtime_error("catalogue unavailable");
  };
  EXPECT_THROW(recover(), std::runtime_error);
  EXPECT_FALSE(activeTracker());
  EXPECT_TRUE(session->isLive());
  EXPECT_TRUE(canUseHardware());
}

TEST_F(DriveSessionLivenessTest, TransferStateTimeoutsAndIdleLivenessRemainUnchanged) {
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

TEST_F(DriveSessionLivenessTest, ConfiguredPhaseTimeoutsDoNotResetOnRepeatedStateReports) {
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

TEST_F(DriveSessionLivenessTest, EachTransferPhaseUsesItsConfiguredTimeout) {
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

TEST_F(DriveSessionLivenessTest, BlockMovementRefreshesTransferLivenessAndNewSessionResetsIt) {
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
