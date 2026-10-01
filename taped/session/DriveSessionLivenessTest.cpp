/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveSession.hpp"
#include "catalogue/dummy/DummyCatalogue.hpp"
#include "common/log/StringLogger.hpp"
#include "taped/SchedulerContext.hpp"
#include "tests/TempFile.hpp"

#include <functional>
#include <gtest/gtest.h>
#include <stdexcept>

#ifndef CTA_PGSCHED
#include "objectstore/BackendVFS.hpp"
#include "objectstore/RootEntry.hpp"

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
  CleanupCatalogue catalogue;
  objectstore::BackendVFS backend;
  unitTests::TempFile schedulerConfig;
  std::unique_ptr<SchedulerContext> schedulerContext;
  std::unique_ptr<DriveSession> session;

  void SetUp() override {
    objectstore::RootEntry root(backend);
    root.initialize();
    root.insert();
    config.drive.name = "drive";
    schedulerConfig.stringFill(backend.getParams()->toURL());
    config.scheduler.config_file = schedulerConfig.path();
    schedulerContext = std::make_unique<SchedulerContext>(config, catalogue, logger);
    session = DriveSession::create(config, logger, *schedulerContext);
  }

  void TearDown() override {
    catalogue.driveState().onRead = {};
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
  catalogue.driveState().onRead = [&] {
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
  catalogue.driveState().onRead = [&] {
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
  catalogue.driveState().onRead = [&] {
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

}  // namespace cta::tape::daemon
#endif
