/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapeDaemon.hpp"

#include "common/exception/LostDatabaseConnection.hpp"
#include "common/exception/TimeoutException.hpp"
#include "common/log/StringLogger.hpp"
#include "common/utils/ScopeExit.hpp"
#include "runtime/config/parsing/TomlParser.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/TapeMount.hpp"
#include "session/TapeSessionWorkerTeardownIncomplete.hpp"

#include <functional>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

namespace cta::tape::daemon {
namespace {
using namespace common::dataStructures;
using testing::_;
using testing::Invoke;
using testing::Return;
using testing::Throw;

class MockDriveScheduler : public IScheduler {
public:
  MOCK_METHOD(void, ping, (log::LogContext&), (override));

  MOCK_METHOD(void, reportDriveStatus, (const DriveInfo&, MountType, DriveStatus, log::LogContext&), (override));

  MOCK_METHOD(void, setDesiredDriveState, (const std::string&, const DesiredDriveState&, log::LogContext&), (override));

  MOCK_METHOD(bool, checkDriveCanBeCreated, (const DriveInfo&, log::LogContext&), (override));

  MOCK_METHOD(DesiredDriveState, getDesiredDriveState, (const std::string&, log::LogContext&), (override));

  MOCK_METHOD(void,
              createTapeDriveStatus,
              (const DriveInfo&,
               const DesiredDriveState&,
               const MountType&,
               const DriveStatus&,
               const SecurityIdentity&,
               log::LogContext&),
              (override));

  MOCK_METHOD(void, reportSchedulerBackendName, (const std::string&, log::LogContext&), (override));
};

class TestMount : public TapeMount {
public:
  std::function<void()> onDestroy;

  ~TestMount() override {
    if (onDestroy) {
      onDestroy();
    }
  }

  MountType getMountType() const override { return MountType::Retrieve; }

  std::string getVid() const override { return "V00001"; }

  std::string getMountTransactionId() const override { return "1"; }

  std::optional<std::string> getActivity() const override { return std::nullopt; }

  uint32_t getNbFiles() const override { return 1; }

  std::string getVo() const override { return "vo"; }

  std::string getMediaType() const override { return "media"; }

  std::string getVendor() const override { return "vendor"; }

  Label::Format getLabelFormat() const override { return Label::Format::CTA; }

  std::string getPoolName() const override { return "pool"; }

  uint64_t getCapacityInBytes() const override { return 1; }

  std::optional<std::string> getEncryptionKeyName() const override { return std::nullopt; }

  void complete() override {}

  void setDriveStatus(DriveStatus, const std::optional<std::string>&) override {}

  void setTapeSessionStats(const TapeTransferStats&) override {}

  void setTapeMounted(log::LogContext&) const override {}
};
}  // namespace

class TapeDaemonTest : public testing::Test {
protected:
  using Clock = TapeSessionTracker::Clock;
  std::shared_ptr<const TapeSessionTracker> activeTracker {nullptr};
  TapedConfig config;
  log::StringLogger logger {"host", "TapeDaemonTest", log::DEBUG};
  testing::StrictMock<MockDriveScheduler> scheduler;
  std::unique_ptr<TapeDaemon> daemon;
  unsigned int probes = 0;
  unsigned int schedules = 0;
  unsigned int schedulerResets = 0;
  unsigned int downRequests = 0;
  std::function<void()> onDownRequest;
  std::function<void()> onSchedulerReset;
  unsigned int transfers = 0;
  unsigned int destroyed = 0;
  TapeMount* m_liveMount = nullptr;
  std::vector<unsigned int> sleeps;
  std::function<void()> onSleep;
  std::function<std::unique_ptr<TapeMount>()> schedule;
  std::function<TapeSessionResult(TapeMount&)> transfer;
  std::function<bool()> libraryExists = [] { return true; };
  std::function<bool()> clean = [] { return true; };
  std::optional<TapeDrive> previousDrive;
  unsigned int stateReads = 0;
  unsigned int cleanings = 0;
  std::optional<std::string> cleanedVid;
  bool waitedForMedia = false;

  class FakeDriveOperations final : public DriveOperations {
  public:
    explicit FakeDriveOperations(TapeDaemonTest& fixture) : fixture(fixture) {}

    std::optional<TapeSessionLivenessSnapshot> tapeSessionLiveness() const override {
      const auto tracker = std::atomic_load(&fixture.activeTracker);
      if (!tracker) {
        return std::nullopt;
      }
      return tracker->livenessSnapshot();
    }

    IScheduler& scheduler() override { return fixture.scheduler; }

    void resetScheduler() override {
      EXPECT_EQ(nullptr, fixture.liveMount());
      ++fixture.schedulerResets;
      if (fixture.onSchedulerReset) {
        fixture.onSchedulerReset();
      }
    }

    void requestDriveDown(log::LogContext&) override {
      ++fixture.downRequests;
      if (fixture.previousDrive) {
        fixture.previousDrive->desiredUp = false;
      }
      if (fixture.onDownRequest) {
        fixture.onDownRequest();
      }
    }

    std::optional<TapeDrive> getDriveState() override {
      ++fixture.stateReads;
      return fixture.previousDrive;
    }

    bool logicalLibraryExists() override { return fixture.libraryExists(); }

    std::pair<bool, std::optional<std::string>> probeDrive() override {
      ++fixture.probes;
      EXPECT_EQ(nullptr, fixture.liveMount());
      ADD_FAILURE() << "Daemon and drive sessions must not probe hardware";
      return {true, std::nullopt};
    }

    std::unique_ptr<TapeMount> getNextMount() override {
      ++fixture.schedules;
      return fixture.schedule ? fixture.schedule() : nullptr;
    }

    TapeSessionResult runTapeSession(TapeMount& tapeMount) override {
      ++fixture.transfers;
      EXPECT_EQ(&tapeMount, fixture.liveMount());
      return fixture.transfer ? fixture.transfer(tapeMount) : TapeSessionResult {};
    }

    bool clean(const std::optional<std::string>& vid, bool waitMediaInDrive) override {
      ++fixture.cleanings;
      fixture.cleanedVid = vid;
      fixture.waitedForMedia = waitMediaInDrive;
      return fixture.clean();
    }

    // Record a requested delay, throwing after excessive waits to prevent runaway tests.
    void sleep(unsigned int seconds) override {
      if (fixture.sleeps.size() >= 10) {
        throw std::runtime_error("Unexpected repeated daemon wait");
      }
      fixture.sleeps.push_back(seconds);
      if (fixture.onSleep) {
        fixture.onSleep();
      }
    }

  private:
    TapeDaemonTest& fixture;
  } operations {*this};

  void SetUp() override {
    config.drive.name = "drive";
    config.mounts.idle_scheduling_interval_secs = 7;
    config.mounts.logical_library_poll_interval_secs = 17;
    daemon = std::make_unique<TapeDaemon>(config, logger, operations);
    previousDrive.emplace();
    previousDrive->driveStatus = DriveStatus::Up;
  }

  bool liveAt(Clock::time_point now) const { return daemon->isLive(now); }

  void waitForLibrary() { daemon->waitForLogicalLibrary(); }

  int shutdown() {
    // Exercise shutdown through the daemon's normal exit path.
    daemon->stop();
    return daemon->run();
  }

  bool registerDrive() { return daemon->registerDrive(false); }

  void waitForUp() { daemon->waitUntilDriveIsRequestedUp(); }

  void expectTransition(bool afterWaiting = true) {
    testing::InSequence sequence;
    DesiredDriveState up;
    up.up = true;
    if (afterWaiting) {
      EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
    }
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
    EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _));
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
    EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Up, _));
  }

  TapeMount* liveMount() { return m_liveMount; }

  void expectRunStartup() {
    EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
    EXPECT_CALL(scheduler, createTapeDriveStatus(_, _, MountType::NoMount, DriveStatus::Down, _, _));
    EXPECT_CALL(scheduler, reportSchedulerBackendName("drive", _));
  }

  void expectShutdown() {
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
    EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
    EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
      .WillOnce(Invoke([](const auto&, const DesiredDriveState& state, auto&) { EXPECT_FALSE(state.up); }));
  }

  void expectSchedulingAttempt() {
    testing::InSequence sequence;
    DesiredDriveState state;
    state.up = true;
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(state));
    EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Up, _));
  }

  void supplyMount() {
    schedule = [this] {
      auto tapeMount = std::make_unique<TestMount>();
      EXPECT_EQ(nullptr, liveMount());
      m_liveMount = tapeMount.get();
      tapeMount->onDestroy = [this] {
        m_liveMount = nullptr;
        ++destroyed;
      };
      return tapeMount;
    };
  }
};

TEST_F(TapeDaemonTest, RecoveryStatusPublicationFailureDoesNotTouchHardware) {
  testing::InSequence sequence;
  previousDrive.emplace();
  previousDrive->driveStatus = DriveStatus::Transferring;
  previousDrive->desiredUp = true;
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _))
    .WillOnce(Throw(std::runtime_error("publication failed")));
  EXPECT_EQ(1, daemon->run());
  EXPECT_FALSE(daemon->isReady());
  EXPECT_EQ(0, cleanings);
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
}

// Verify an operator down request delays cleanup until a subsequent up request.
TEST_F(TapeDaemonTest, RegistrationConflictStopsStartup) {
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(false));
  EXPECT_EQ(1, daemon->run());
  EXPECT_FALSE(daemon->isReady());
  EXPECT_EQ(0, schedules);
}

// If the drive already has an operator reason and comment, registration preserves both.
TEST_F(TapeDaemonTest, RegistrationPreservesOperatorReasonAndComment) {
  DesiredDriveState state;
  state.up = false;
  state.reason = "Operator intervention";
  state.comment = "Inspect drive";
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(state));
  EXPECT_CALL(scheduler, createTapeDriveStatus(_, _, MountType::NoMount, DriveStatus::Down, _, _))
    .WillOnce(Invoke([&](const auto&, const DesiredDriveState& desired, const auto&, const auto&, const auto&, auto&) {
      EXPECT_FALSE(desired.up);
      EXPECT_EQ(state.reason, desired.reason);
      EXPECT_EQ(state.comment, desired.comment);
    }));
  EXPECT_CALL(scheduler, reportSchedulerBackendName("drive", _));
  EXPECT_TRUE(registerDrive());
}

// Preserve an up request that arrived after the initial catalogue snapshot.
TEST_F(TapeDaemonTest, RegistrationPreservesNewUpRequestAndPublishesReadinessLast) {
  DesiredDriveState state;
  state.up = true;
  state.reason = "Setting drive up";
  state.comment = "Operator comment";
  EXPECT_FALSE(daemon->isReady());
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(state));
  EXPECT_CALL(scheduler, createTapeDriveStatus(_, _, MountType::NoMount, DriveStatus::Down, _, _))
    .WillOnce(Invoke([&](const auto&, const DesiredDriveState& desired, const auto&, const auto&, const auto&, auto&) {
      EXPECT_FALSE(daemon->isReady());
      EXPECT_TRUE(desired.up);
      EXPECT_EQ(state.reason, desired.reason);
      EXPECT_EQ(state.comment, desired.comment);
    }));
  EXPECT_CALL(scheduler, reportSchedulerBackendName("drive", _)).WillOnce(Invoke([&](const auto&, auto&) {
    EXPECT_FALSE(daemon->isReady());
  }));
  EXPECT_TRUE(registerDrive());
  EXPECT_TRUE(daemon->isReady());
}

TEST_F(TapeDaemonTest, RegistrationReplacesAbsentAndCleanShutdownReasonsWithStartup) {
  // Check both states that should receive a fresh startup reason.
  for (const auto& reason :
       std::vector<std::optional<std::string>> {std::nullopt, formatDriveDownReason(DriveDownReason::Shutdown)}) {
    SCOPED_TRACE(reason.value_or("absent"));
    DesiredDriveState state;
    state.reason = reason;

    // Capture the state published to the catalogue during registration.
    EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(state));
    EXPECT_CALL(scheduler, createTapeDriveStatus(_, _, MountType::NoMount, DriveStatus::Down, _, _))
      .WillOnce(Invoke([](const auto&, const DesiredDriveState& desired, const auto&, const auto&, const auto&, auto&) {
        EXPECT_FALSE(desired.up);
        EXPECT_EQ(formatDriveDownReason(DriveDownReason::Startup), desired.reason);
      }));
    EXPECT_CALL(scheduler, reportSchedulerBackendName("drive", _));

    EXPECT_TRUE(registerDrive());

    // Remove this case's expectations before setting up the next reason.
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&scheduler));
  }
}

// A deleted catalogue entry is left for the next daemon run to recreate.
TEST_F(TapeDaemonTest, MissingDriveEndsRunWithoutRegistrationOrDownPublication) {
  testing::InSequence sequence;
  expectRunStartup();
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Throw(Scheduler::NoSuchDrive("missing")));

  EXPECT_EQ(1, daemon->run());
  EXPECT_FALSE(daemon->isReady());
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, cleanings);
  EXPECT_EQ(0, schedules);
  EXPECT_TRUE(sleeps.empty());
  EXPECT_THAT(logger.getLog(), testing::HasSubstr("Drive is missing from the catalogue. Exiting."));
}

TEST_F(TapeDaemonTest, MissingDrivePropagatesFromWaitForUp) {
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Throw(Scheduler::NoSuchDrive("missing")));
  EXPECT_THROW(waitForUp(), Scheduler::NoSuchDrive);
}

// If publishing the drive registration fails, run should end with a failure result.
TEST_F(TapeDaemonTest, RegistrationPublicationFailureAbortsStartupBeforeLibraryOrScheduling) {
  testing::InSequence sequence;
  unsigned int libraryChecks = 0;
  libraryExists = [&] {
    ++libraryChecks;
    return true;
  };
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, createTapeDriveStatus(_, _, MountType::NoMount, DriveStatus::Down, _, _))
    .WillOnce(Throw(std::runtime_error("registration publication failed")));

  EXPECT_EQ(1, daemon->run());
  EXPECT_FALSE(daemon->isReady());
  EXPECT_EQ(0, libraryChecks);
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
}

// If publishing the scheduler backend name fails, run should end with a failure result.
TEST_F(TapeDaemonTest, SchedulerBackendPublicationFailureAbortsStartupBeforeLibraryOrScheduling) {
  testing::InSequence sequence;
  unsigned int libraryChecks = 0;
  libraryExists = [&] {
    ++libraryChecks;
    return true;
  };
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, createTapeDriveStatus(_, _, MountType::NoMount, DriveStatus::Down, _, _));
  EXPECT_CALL(scheduler, reportSchedulerBackendName("drive", _))
    .WillOnce(Throw(std::runtime_error("backend publication failed")));

  EXPECT_EQ(1, daemon->run());
  EXPECT_FALSE(daemon->isReady());
  EXPECT_EQ(0, libraryChecks);
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
}

// If registration loses its database connection, run should end with a failure result.
TEST_F(TapeDaemonTest, StartupDatabaseFailureAbortsBeforeLibraryOrScheduling) {
  clean = [] {
    ADD_FAILURE() << "Startup failure must not touch tape hardware";
    return true;
  };
  unsigned int libraryChecks = 0;
  libraryExists = [&] {
    ++libraryChecks;
    return true;
  };
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _))
    .WillOnce(Throw(exception::LostDatabaseConnection("objectstore unavailable")));

  EXPECT_EQ(1, daemon->run());
  EXPECT_FALSE(daemon->isReady());
  EXPECT_EQ(0, libraryChecks);
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
}

// If the configured logical library is absent, startup waits for it to appear.
TEST_F(TapeDaemonTest, MissingLogicalLibraryWaitsUntilAvailable) {
  unsigned int checks = 0;
  libraryExists = [&] { return ++checks == 3; };
  waitForLibrary();
  EXPECT_EQ(3, checks);
  EXPECT_THAT(sleeps,
              testing::ElementsAre(config.mounts.logical_library_poll_interval_secs,
                                   config.mounts.logical_library_poll_interval_secs));
  EXPECT_EQ(0, schedules);
}

// If the logical-library lookup loses its database connection, startup propagates the error.
TEST_F(TapeDaemonTest, LogicalLibraryDatabaseFailurePropagatesWithoutWaiting) {
  libraryExists = []() -> bool { throw exception::LostDatabaseConnection("database unavailable"); };
  EXPECT_THROW(waitForLibrary(), exception::LostDatabaseConnection);
  EXPECT_TRUE(sleeps.empty());
}

// If the logical-library lookup loses its database connection, run should end with a failure result.
TEST_F(TapeDaemonTest, LogicalLibraryDatabaseFailureEndsStartupWithoutScheduling) {
  testing::InSequence sequence;
  libraryExists = []() -> bool { throw exception::LostDatabaseConnection("objectstore unavailable"); };
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, createTapeDriveStatus(_, _, MountType::NoMount, DriveStatus::Down, _, _));
  EXPECT_CALL(scheduler, reportSchedulerBackendName("drive", _));

  expectShutdown();
  EXPECT_EQ(1, daemon->run());
  EXPECT_FALSE(daemon->isReady());
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
}

// Only failures after successful registration publish down on exit.
TEST_F(TapeDaemonTest, UnknownStartupFailuresPublishDownOnlyAfterRegistration) {
  for (const bool duringRegistration : {false, true}) {
    SCOPED_TRACE(duringRegistration);
    testing::InSequence sequence;
    EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
    if (duringRegistration) {
      EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Throw(42));
    } else {
      EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
      EXPECT_CALL(scheduler, createTapeDriveStatus(_, _, MountType::NoMount, DriveStatus::Down, _, _));
      EXPECT_CALL(scheduler, reportSchedulerBackendName("drive", _));
      libraryExists = []() -> bool { throw 42; };
    }
    if (!duringRegistration) {
      expectShutdown();
    }
    EXPECT_EQ(1, daemon->run());
    EXPECT_FALSE(daemon->isReady());
    EXPECT_EQ(0, probes);
    EXPECT_EQ(0, cleanings);
    EXPECT_EQ(0, transfers);
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&scheduler));
  }
}

TEST_F(TapeDaemonTest, UnknownOwnershipFailureDoesNotPublishOrTouchHardware) {
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Throw(42));
  EXPECT_EQ(1, daemon->run());
  EXPECT_FALSE(daemon->isReady());
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, cleanings);
  EXPECT_EQ(0, transfers);
}

TEST_F(TapeDaemonTest, DownWaitingAndExitNeverTouchHardware) {
  testing::InSequence sequence;
  expectRunStartup();
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Throw(std::runtime_error("database unavailable")));
  expectShutdown();
  EXPECT_EQ(1, daemon->run());
  EXPECT_FALSE(daemon->isReady());
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, cleanings);
  EXPECT_EQ(0, schedules);
  EXPECT_EQ(0, transfers);
  EXPECT_EQ(2, sleeps.size());
}

TEST_F(TapeDaemonTest, OperatorDownUpStartsAnotherCleanedUpPeriod) {
  testing::InSequence sequence;
  expectRunStartup();
  expectTransition();
  expectSchedulingAttempt();
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  expectTransition();
  expectSchedulingAttempt();
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  expectShutdown();
  onSleep = [&] {
    if (schedules == 2) {
      daemon->stop();
    }
  };
  EXPECT_EQ(0, daemon->run());
  EXPECT_EQ(2, cleanings);
  EXPECT_EQ(2, schedules);
  EXPECT_EQ(0, probes);
}

TEST_F(TapeDaemonTest, SchedulerRetiredAfterMountDestructionAndBeforeDelay) {
  testing::InSequence sequence;
  expectRunStartup();
  expectTransition();
  expectSchedulingAttempt();
  expectSchedulingAttempt();
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  expectShutdown();
  supplyMount();
  transfer = [&](TapeMount&) { return TapeSessionResult {.successful = transfers == 1}; };
  onSchedulerReset = [&] { EXPECT_EQ(transfers, destroyed); };
  onSleep = [&] {
    EXPECT_EQ(2, schedulerResets);
    EXPECT_EQ(2, destroyed);
    daemon->stop();
  };
  EXPECT_EQ(0, daemon->run());
  EXPECT_EQ(2, schedulerResets);
  EXPECT_THAT(sleeps, testing::ElementsAre(config.mounts.idle_scheduling_interval_secs));
}

TEST_F(TapeDaemonTest, IdlePollResetsBeforeRetryDelay) {
  testing::InSequence sequence;
  expectRunStartup();
  expectTransition();
  expectSchedulingAttempt();
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  expectShutdown();
  onSleep = [&] {
    EXPECT_EQ(1, schedulerResets);
    daemon->stop();
  };
  EXPECT_EQ(0, daemon->run());
  EXPECT_EQ(1, schedulerResets);
  EXPECT_EQ(0, transfers);
}

TEST_F(TapeDaemonTest, UnusableSessionResetsBeforeLeavingUpPeriod) {
  testing::InSequence sequence;
  expectRunStartup();
  expectTransition();
  expectSchedulingAttempt();
  expectShutdown();  // The session result requests down.
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  expectShutdown();  // The stop request then shuts down the daemon.
  supplyMount();
  transfer = [&](TapeMount&) {
    daemon->stop();
    return TapeSessionResult {.driveReusable = false};
  };
  onSchedulerReset = [&] { EXPECT_EQ(1, destroyed); };
  EXPECT_EQ(0, daemon->run());
  EXPECT_EQ(1, schedulerResets);
  EXPECT_TRUE(sleeps.empty());
}

TEST_F(TapeDaemonTest, IdlePollResetFailureExitsWithoutRetrySleep) {
  testing::InSequence sequence;
  expectRunStartup();
  expectTransition();
  expectSchedulingAttempt();
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  expectShutdown();
  onSchedulerReset = [] { throw std::runtime_error("scheduler replacement failed"); };
  EXPECT_EQ(1, daemon->run());
  EXPECT_EQ(1, schedules);
  EXPECT_EQ(0, transfers);
  EXPECT_EQ(1, schedulerResets);
  EXPECT_TRUE(sleeps.empty());
}

TEST_F(TapeDaemonTest, SchedulingFailureResetFailurePropagatesWithoutRetrySleep) {
  for (const bool timeout : {false, true}) {
    SCOPED_TRACE(timeout);
    testing::InSequence sequence;
    expectRunStartup();
    expectTransition();
    expectSchedulingAttempt();
    EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
    expectShutdown();
    schedule = [timeout]() -> std::unique_ptr<TapeMount> {
      if (timeout) {
        throw exception::TimeoutException("timeout");
      }
      throw std::runtime_error("scheduling failed");
    };
    onSchedulerReset = [] { throw std::runtime_error("scheduler replacement failed"); };
    EXPECT_EQ(1, daemon->run());
    EXPECT_EQ(0, transfers);
    EXPECT_TRUE(sleeps.empty());
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&scheduler));
  }
  EXPECT_EQ(2, schedulerResets);
}

TEST_F(TapeDaemonTest, SchedulerResetFailurePublishesDownAndExits) {
  testing::InSequence sequence;
  expectRunStartup();
  expectTransition();
  expectSchedulingAttempt();
  supplyMount();
  onSchedulerReset = [] { throw std::runtime_error("scheduler replacement failed"); };
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _));

  EXPECT_EQ(1, daemon->run());
  EXPECT_EQ(1, schedulerResets);
  EXPECT_EQ(1, destroyed);
  EXPECT_FALSE(daemon->isReady());
  EXPECT_TRUE(sleeps.empty());
}

TEST_F(TapeDaemonTest, RecoveredSessionSchedulesAgainWithoutAnotherTransition) {
  testing::InSequence sequence;
  expectRunStartup();
  expectTransition();
  expectSchedulingAttempt();
  expectTransition(false);
  expectSchedulingAttempt();
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  expectShutdown();
  supplyMount();
  transfer = [&](TapeMount&) -> TapeSessionResult {
    if (transfers == 1) {
      throw std::runtime_error("session failed");
    }
    daemon->stop();
    return {};
  };
  EXPECT_EQ(0, daemon->run());
  EXPECT_EQ(2, transfers);
  EXPECT_EQ(2, cleanings);  // Startup and exception recovery only.
  EXPECT_EQ(2, destroyed);
  EXPECT_EQ(2, schedulerResets);
  EXPECT_THAT(sleeps, testing::ElementsAre(config.mounts.idle_scheduling_interval_secs));
}

TEST_F(TapeDaemonTest, StopDuringFailedSessionCurrentlyDefersCleanup) {
  testing::InSequence sequence;
  expectRunStartup();
  expectTransition();
  expectSchedulingAttempt();
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  expectShutdown();
  supplyMount();
  transfer = [&](TapeMount&) -> TapeSessionResult {
    daemon->stop();
    throw std::runtime_error("session failed while stopping");
  };
  EXPECT_EQ(0, daemon->run());
  EXPECT_EQ(1, cleanings);  // TODO: recovery is skipped once stop publishes desired-down.
  EXPECT_EQ(1, downRequests);
  EXPECT_EQ(1, schedules);
  EXPECT_EQ(1, destroyed);
  EXPECT_TRUE(sleeps.empty());
}

TEST_F(TapeDaemonTest, IterationExceptionPublishesDownWithoutCleaning) {
  testing::InSequence sequence;
  expectRunStartup();
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Throw(std::runtime_error("iteration failed")));
  clean = [] {
    ADD_FAILURE() << "Shutdown must not clean a drive that may be used by an operator";
    return false;
  };
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& state, auto&) {
      EXPECT_FALSE(state.up);
      EXPECT_EQ(formatDriveDownReason(DriveDownReason::Shutdown), state.reason);
    }));

  EXPECT_EQ(1, daemon->run());
  EXPECT_FALSE(daemon->isReady());
  EXPECT_EQ(0, cleanings);
  EXPECT_EQ(0, probes);
  EXPECT_THAT(logger.getLog(), testing::HasSubstr("Tape daemon failed. Publishing down state before exit."));
}

// Incomplete worker teardown is fatal and cannot be repaired by daemon cleanup.
TEST_F(TapeDaemonTest, IncompleteWorkerTeardownExitsWithoutRecovery) {
  testing::InSequence sequence;
  expectRunStartup();
  expectTransition();
  expectSchedulingAttempt();
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& state, auto&) {
      EXPECT_FALSE(state.up);
      EXPECT_EQ(formatDriveDownReason(DriveDownReason::SessionDidNotStopSafely), state.reason);
    }));
  supplyMount();
  transfer = [](TapeMount&) -> TapeSessionResult {
    try {
      throw 42;
    } catch (...) {
      std::throw_with_nested(TapeSessionWorkerTeardownIncomplete());
    }
  };
  EXPECT_EQ(1, daemon->run());
  EXPECT_EQ(1, cleanings);  // Only the initial transition; no recovery with live workers.
  EXPECT_EQ(1, destroyed);
  EXPECT_EQ(0, schedulerResets);
  EXPECT_EQ(1, transfers);
  EXPECT_FALSE(daemon->isReady());
}

TEST_F(TapeDaemonTest, IncompleteWorkerTeardownPreservesExistingReason) {
  testing::InSequence sequence;
  expectRunStartup();
  expectTransition();
  expectSchedulingAttempt();
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  DesiredDriveState state;
  state.reason = "Operator intervention";
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(state));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& desired, auto&) {
      EXPECT_FALSE(desired.up);
      // An absent reason leaves the existing catalogue reason intact.
      EXPECT_FALSE(desired.reason);
    }));
  supplyMount();
  transfer = [](TapeMount&) -> TapeSessionResult { throw TapeSessionWorkerTeardownIncomplete(); };

  EXPECT_EQ(1, daemon->run());
  EXPECT_EQ(1, cleanings);
  EXPECT_EQ(0, schedulerResets);
  EXPECT_FALSE(daemon->isReady());
}

// Shutdown publishes down without invoking the cleaner.
TEST_F(TapeDaemonTest, ShutdownPublishesDownWithoutCleaning) {
  testing::InSequence sequence;
  expectRunStartup();
  clean = [] {
    ADD_FAILURE() << "Shutdown must not clean the drive";
    return false;
  };
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& state, auto&) {
      EXPECT_FALSE(state.up);
      EXPECT_EQ(formatDriveDownReason(DriveDownReason::Shutdown), state.reason);
    }));
  EXPECT_EQ(0, shutdown());
  EXPECT_EQ(0, cleanings);
}

// A reported-state publication failure must not prevent the desired-state publication.
TEST_F(TapeDaemonTest, ShutdownAttemptsBothDownPublicationsAfterFailure) {
  testing::InSequence sequence;
  expectRunStartup();
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _))
    .WillOnce(Throw(std::runtime_error("reported state failed")));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _));
  EXPECT_EQ(1, shutdown());
  EXPECT_EQ(0, cleanings);
}

// A desired-state publication failure makes shutdown return failure.
TEST_F(TapeDaemonTest, ShutdownPublicationFailureReturnsNonzero) {
  testing::InSequence sequence;
  expectRunStartup();
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _)).WillOnce(Throw(std::runtime_error("publication failed")));
  EXPECT_EQ(1, shutdown());
  EXPECT_EQ(0, cleanings);
}

// Unknown publication failures follow the same best-effort exit policy as standard exceptions.
TEST_F(TapeDaemonTest, ShutdownContainsUnknownFailuresAndAttemptsBothPublications) {
  for (const bool failReported : {false, true}) {
    for (const bool failDesired : {false, true}) {
      testing::InSequence sequence;
      expectRunStartup();
      EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
      EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _))
        .WillOnce(Invoke([&](const auto&, auto, auto, auto&) {
          if (failReported) {
            throw 42;
          }
        }));
      EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _)).WillOnce(Invoke([&](const auto&, const auto&, auto&) {
        if (failDesired) {
          throw 43;
        }
      }));
      EXPECT_EQ(failReported || failDesired ? 1 : 0, shutdown());
      EXPECT_EQ(0, probes);
      EXPECT_EQ(0, cleanings);
      ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&scheduler));
    }
  }
}

// If shutdown finds no active failure reason, it publishes a clean-shutdown reason.
TEST_F(TapeDaemonTest, ShutdownReplacesStartupAndCleanReasonsButPreservesOperatorReason) {
  for (const auto& reason : std::vector<std::string> {"",
                                                      formatDriveDownReason(DriveDownReason::Startup),
                                                      formatDriveDownReason(DriveDownReason::Shutdown),
                                                      "Operator intervention"}) {
    SCOPED_TRACE(reason);
    testing::InSequence sequence;
    expectRunStartup();
    DesiredDriveState state;
    state.reason = reason;
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(state));
    EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
    EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
      .WillOnce(Invoke([&](const auto&, const DesiredDriveState& desired, auto&) {
        EXPECT_FALSE(desired.up);
        if (reason == "Operator intervention") {
          EXPECT_FALSE(desired.reason);
        } else {
          EXPECT_EQ(formatDriveDownReason(DriveDownReason::Shutdown), desired.reason);
        }
      }));
    EXPECT_EQ(0, shutdown());
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&scheduler));
  }
}

TEST_F(TapeDaemonTest, StopBeforeRegistrationDoesNotPublishDesiredDown) {
  daemon->stop();
  EXPECT_EQ(0, downRequests);
  EXPECT_EQ(0, schedules);
}

TEST_F(TapeDaemonTest, StopPublishesDesiredDownOnceWithoutTouchingHardware) {
  expectRunStartup();
  ASSERT_TRUE(registerDrive());
  previousDrive->desiredUp = true;
  previousDrive->driveStatus = DriveStatus::Transferring;
  daemon->stop();
  daemon->stop();
  EXPECT_EQ(1, downRequests);
  EXPECT_FALSE(previousDrive->desiredUp);
  EXPECT_EQ(DriveStatus::Transferring, previousDrive->driveStatus);
  EXPECT_EQ(0, cleanings);
  EXPECT_EQ(0, probes);
}

TEST_F(TapeDaemonTest, StopPublicationFailureStillExits) {
  for (const bool unknown : {false, true}) {
    SCOPED_TRACE(unknown);
    daemon = std::make_unique<TapeDaemon>(config, logger, operations);
    testing::InSequence sequence;
    expectRunStartup();
    expectShutdown();
    onDownRequest = [unknown] {
      if (unknown) {
        throw 42;
      }
      throw std::runtime_error("catalogue unavailable");
    };
    libraryExists = [&] {
      EXPECT_NO_THROW(daemon->stop());
      return true;
    };
    EXPECT_EQ(0, daemon->run());
    EXPECT_FALSE(daemon->isReady());
    EXPECT_EQ(0, schedules);
    EXPECT_EQ(0, cleanings);
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&scheduler));
  }
  EXPECT_EQ(2, downRequests);
}

TEST_F(TapeDaemonTest, StopCanPublishDesiredDownDuringSchedulerReset) {
  testing::InSequence sequence;
  expectRunStartup();
  expectTransition();
  expectSchedulingAttempt();
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  expectShutdown();
  onSchedulerReset = [&] {
    // Exercise the stop callback on another thread while the scheduler is being replaced.
    std::thread stopping([&] { daemon->stop(); });
    stopping.join();
    EXPECT_EQ(1, downRequests);
  };
  EXPECT_EQ(0, daemon->run());
  EXPECT_EQ(1, schedulerResets);
  EXPECT_TRUE(sleeps.empty());
}

TEST_F(TapeDaemonTest, StopWhileWaitingForLibraryExitsWithoutHardwareAccess) {
  testing::InSequence sequence;
  expectRunStartup();
  expectShutdown();
  libraryExists = [] { return false; };
  onSleep = [&] { daemon->stop(); };

  EXPECT_EQ(0, daemon->run());
  EXPECT_FALSE(daemon->isReady());
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, cleanings);
  EXPECT_EQ(0, schedules);
  EXPECT_THAT(sleeps, testing::ElementsAre(config.mounts.logical_library_poll_interval_secs));
}

TEST_F(TapeDaemonTest, StopWhileWaitingForUpExitsWithoutHardwareAccess) {
  testing::InSequence sequence;
  expectRunStartup();
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  expectShutdown();
  onSleep = [&] { daemon->stop(); };

  EXPECT_EQ(0, daemon->run());
  EXPECT_FALSE(daemon->isReady());
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, cleanings);
  EXPECT_EQ(0, schedules);
  EXPECT_THAT(sleeps, testing::ElementsAre(config.mounts.drive_state_poll_interval_secs));
}

TEST_F(TapeDaemonTest, StopDuringIdleSleepPublishesDownAndExits) {
  testing::InSequence sequence;
  expectRunStartup();
  expectTransition();
  expectSchedulingAttempt();
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  expectShutdown();
  onSleep = [&] { daemon->stop(); };

  EXPECT_EQ(0, daemon->run());
  EXPECT_EQ(1, schedules);
  EXPECT_EQ(0, transfers);
  EXPECT_FALSE(daemon->isReady());
}

TEST_F(TapeDaemonTest, StopDuringSessionAllowsCompletionBeforeShutdown) {
  testing::InSequence sequence;
  expectRunStartup();
  expectTransition();
  expectSchedulingAttempt();
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  expectShutdown();
  supplyMount();
  bool completed = false;
  transfer = [&](TapeMount&) {
    daemon->stop();
    EXPECT_NE(nullptr, liveMount());
    completed = true;
    return TapeSessionResult {};
  };

  EXPECT_EQ(0, daemon->run());
  EXPECT_TRUE(completed);
  EXPECT_EQ(1, transfers);
  EXPECT_EQ(1, destroyed);
  EXPECT_FALSE(daemon->isReady());
}

TEST_F(TapeDaemonTest, StopStillReturnsFailureWhenShutdownPublicationFails) {
  testing::InSequence sequence;
  expectRunStartup();
  libraryExists = [&] {
    daemon->stop();
    return true;
  };
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _))
    .WillOnce(Throw(std::runtime_error("publication failed")));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _));

  EXPECT_EQ(1, daemon->run());
  EXPECT_FALSE(daemon->isReady());
  EXPECT_EQ(0, probes);
}

TEST_F(TapeDaemonTest, LivenessUsesEachStateTimeoutAtItsBoundary) {
  using enum cta::tape::session::TapeSessionState;
  using namespace std::chrono_literals;
  config.mounts.mount_timeout_secs = 11;
  config.mounts.tape_load_timeout_secs = 12;
  config.mounts.tape_unload_timeout_secs = 13;
  config.mounts.unmount_timeout_secs = 14;
  config.transfers.no_block_move_timeout_secs = 15;
  config.transfers.retrieve.drain_to_disk_timeout_secs = 16;
  const auto start = Clock::time_point {} + 100s;
  EXPECT_TRUE(liveAt(start));
  auto tracker = std::make_shared<TapeSessionTracker>();
  std::atomic_store<const TapeSessionTracker>(&activeTracker, tracker);
  const utils::ScopeExit clearActiveTracker(
    [this] { std::atomic_store<const TapeSessionTracker>(&activeTracker, nullptr); });
  EXPECT_TRUE(liveAt(start));  // A published tracker may not have started yet.
  tracker->beginTapeSession(start);
  const std::pair<cta::tape::session::TapeSessionState, unsigned int> cases[] {
    {Mounting,       11},
    {Loading,        12},
    {Unloading,      13},
    {Unmounting,     14},
    {Transferring,   15},
    {DrainingToDisk, 16}
  };
  for (const auto& [state, seconds] : cases) {
    tracker->reportState(state, start);
    const auto deadline = start + std::chrono::seconds(seconds);
    EXPECT_TRUE(liveAt(deadline - 1ns));
    EXPECT_FALSE(liveAt(deadline));
    tracker->reportState(state, deadline);
    EXPECT_FALSE(liveAt(deadline));  // Repeated reports must not postpone expiry.
    tracker->updateTapeTransferStats({.dataVolume = 100});
    EXPECT_FALSE(liveAt(deadline));  // Statistics reporting is not block movement.
  }
  for (const auto state : {Preparing, Finalizing, Finished}) {
    tracker->reportState(state, start);
    EXPECT_TRUE(liveAt(start + 24h));
  }
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
  EXPECT_EQ(0, stateReads);
}

TEST_F(TapeDaemonTest, TransferProgressRefreshesOnlyInactivityAndNewSessionResetsIt) {
  using enum cta::tape::session::TapeSessionState;
  using namespace std::chrono_literals;
  const auto start = Clock::time_point {} + 100s;
  config.transfers.no_block_move_timeout_secs = 10;
  auto tracker = std::make_shared<TapeSessionTracker>();
  std::atomic_store<const TapeSessionTracker>(&activeTracker, tracker);
  const utils::ScopeExit clearActiveTracker(
    [this] { std::atomic_store<const TapeSessionTracker>(&activeTracker, nullptr); });
  tracker->beginTapeSession(start);
  tracker->notifyBlockMovement(1, start + 1s);
  tracker->reportState(Transferring, start + 2s);
  EXPECT_TRUE(liveAt(start + 11s));
  EXPECT_FALSE(liveAt(start + 12s));
  tracker->notifyBlockMovement(1, start + 12s);
  EXPECT_TRUE(liveAt(start + 12s));  // An expired check is not latched.
  EXPECT_FALSE(liveAt(start + 22s));
  tracker->beginTapeSession(start + 30s);
  tracker->reportState(Transferring, start + 30s);
  EXPECT_TRUE(liveAt(start + 39s));
  EXPECT_FALSE(liveAt(start + 40s));
}

TEST_F(TapeDaemonTest, ActiveTrackerIsOwnedAndClearedOnReturnAndException) {
  using namespace std::chrono_literals;
  const auto start = Clock::time_point {} + 100s;
  config.mounts.mount_timeout_secs = 1;
  for (const bool fail : {false, true}) {
    std::weak_ptr<TapeSessionTracker> weak;
    const auto execute = [&] {
      auto tracker = std::make_shared<TapeSessionTracker>();
      weak = tracker;
      tracker->beginTapeSession(start);
      tracker->reportState(cta::tape::session::TapeSessionState::Mounting, start);
      std::atomic_store<const TapeSessionTracker>(&activeTracker, tracker);
      const utils::ScopeExit clearActiveTracker(
        [this] { std::atomic_store<const TapeSessionTracker>(&activeTracker, nullptr); });
      tracker.reset();
      EXPECT_FALSE(weak.expired());
      EXPECT_FALSE(liveAt(start + 1s));
      if (fail) {
        throw std::runtime_error("session failed");
      }
    };
    if (fail) {
      EXPECT_THROW(execute(), std::runtime_error);
    } else {
      EXPECT_NO_THROW(execute());
    }
    EXPECT_TRUE(weak.expired());
    EXPECT_FALSE(std::atomic_load(&activeTracker));
    EXPECT_TRUE(liveAt(start + 1s));
  }
}

TEST_F(TapeDaemonTest, UnloadTimeoutDefaultsAndValidation) {
  EXPECT_EQ(900, config.mounts.tape_unload_timeout_secs);
  EXPECT_TRUE(config.mounts.validate().ok());
  config.mounts.tape_unload_timeout_secs = 0;
  EXPECT_FALSE(config.mounts.validate().ok());
  EXPECT_NE(std::string::npos, config.mounts.validate().what().find("tape_unload_timeout_secs"));
  config.mounts.tape_unload_timeout_secs = 1;
  EXPECT_TRUE(config.mounts.validate().ok());
}

TEST_F(TapeDaemonTest, StartupRecoveryDoesNotTrustCatalogueVid) {
  testing::InSequence sequence;
  previousDrive->desiredUp = true;
  previousDrive->driveStatus = DriveStatus::Transferring;
  previousDrive->currentVid = "INTERRUPTED";
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _))
    .WillOnce(Invoke([&](const auto&, auto, auto, auto&) { previousDrive->currentVid.reset(); }));
  EXPECT_CALL(scheduler, reportSchedulerBackendName("drive", _));
  expectTransition();
  expectSchedulingAttempt();
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  expectShutdown();
  onSleep = [&] { daemon->stop(); };
  EXPECT_EQ(0, daemon->run());
  EXPECT_FALSE(cleanedVid);
  EXPECT_EQ(1, cleanings);
  EXPECT_EQ(1, schedules);
}

TEST_F(TapeDaemonTest, FailedDriveSessionPreparationWaitsForAnotherUpRequest) {
  testing::InSequence sequence;
  expectRunStartup();
  DesiredDriveState up;
  up.up = true;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _));
  expectShutdown();  // Failed cleaning requests down.
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));  // Constructor rollback.
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  expectTransition();
  expectSchedulingAttempt();
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  expectShutdown();
  clean = [&] { return cleanings > 1; };
  onSleep = [&] {
    if (schedules) {
      daemon->stop();
    }
  };
  EXPECT_EQ(0, daemon->run());
  EXPECT_EQ(2, cleanings);
  EXPECT_EQ(1, schedules);
  EXPECT_EQ(2, sleeps.size());
}

TEST_F(TapeDaemonTest, ReportedDownWithPendingUpStartsAnotherDriveSession) {
  testing::InSequence sequence;
  expectRunStartup();
  expectTransition();
  expectSchedulingAttempt();
  DesiredDriveState up;
  up.up = true;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  expectTransition();
  expectSchedulingAttempt();
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  expectShutdown();
  clean = [&] {
    previousDrive->driveStatus = DriveStatus::Up;
    return true;
  };
  onSleep = [&] {
    if (schedules == 1) {
      previousDrive->driveStatus = DriveStatus::Down;
    } else {
      daemon->stop();
    }
  };
  EXPECT_EQ(0, daemon->run());
  EXPECT_EQ(2, cleanings);
  EXPECT_EQ(2, schedules);
}

}  // namespace cta::tape::daemon
