/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveController.hpp"

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

class DriveControllerTest : public testing::Test {
protected:
  using Clock = TapeSessionTracker::Clock;
  std::shared_ptr<const TapeSessionTracker> activeTracker {nullptr};
  TapedConfig config;
  log::StringLogger logger {"host", "DriveControllerTest", log::DEBUG};
  testing::StrictMock<MockDriveScheduler> scheduler;
  std::unique_ptr<DriveController> controller;
  testing::Expectation preparationComplete;
  bool empty = true;
  std::optional<std::string> probeError;
  unsigned int probes = 0;
  unsigned int schedules = 0;
  unsigned int schedulerResets = 0;
  std::function<void()> onSchedulerReset;
  unsigned int transfers = 0;
  unsigned int destroyed = 0;
  TapeMount* m_liveMount = nullptr;
  std::vector<unsigned int> sleeps;
  std::function<void()> onSleep;
  std::function<std::unique_ptr<TapeMount>()> schedule;
  std::function<TapeSessionResult(TapeMount&)> transfer;
  std::function<bool()> libraryExists = [] { return true; };
  std::function<void()> probe;
  std::function<bool()> clean = [] { return true; };
  std::optional<TapeDrive> previousDrive;
  unsigned int stateReads = 0;
  unsigned int cleanings = 0;
  std::optional<std::string> cleanedVid;
  bool waitedForMedia = false;

  class FakeDriveOperations final : public DriveOperations {
  public:
    explicit FakeDriveOperations(DriveControllerTest& fixture) : fixture(fixture) {}

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

    std::optional<TapeDrive> getDriveState() override {
      ++fixture.stateReads;
      return fixture.previousDrive;
    }

    bool logicalLibraryExists() override { return fixture.libraryExists(); }

    std::pair<bool, std::optional<std::string>> probeDrive() override {
      ++fixture.probes;
      EXPECT_EQ(nullptr, fixture.liveMount());
      if (fixture.probe) {
        fixture.probe();
      }
      return {fixture.empty, fixture.probeError};
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
        throw std::runtime_error("Unexpected repeated controller wait");
      }
      fixture.sleeps.push_back(seconds);
      if (fixture.onSleep) {
        fixture.onSleep();
      }
    }

  private:
    DriveControllerTest& fixture;
  } operations {*this};

  void SetUp() override {
    config.drive.name = "drive";
    config.mounts.idle_scheduling_interval_secs = 7;
    config.mounts.logical_library_poll_interval_secs = 17;
    controller = std::make_unique<DriveController>(config, logger, operations);
    previousDrive.emplace();
    previousDrive->driveStatus = DriveStatus::Up;
  }

  bool liveAt(Clock::time_point now) const { return controller->isLive(now); }

  void waitForLibrary() { controller->waitForLogicalLibrary(); }

  TapeSessionResult iterationResult;

  bool iteration() {
    iterationResult = controller->runIteration();
    return iterationResult.driveReusable;
  }

  int shutdown() {
    // Exercise shutdown through the controller's normal exit path.
    controller->stop();
    return controller->run();
  }

  bool registerDrive() { return controller->registerDrive(false); }

  void waitForUp() { controller->waitUntilDriveIsRequestedUp(); }

  bool prepare() {
    controller->waitUntilDriveIsRequestedUp();
    return controller->onDownToUpTransition();
  }

  void expectTransition() {
    testing::InSequence sequence;
    DesiredDriveState up;
    up.up = true;
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
    EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _));
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
    EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Up, _));
  }

  TapeMount* liveMount() { return m_liveMount; }

  void down(bool preserve = false) { controller->putDriveDown(DriveDownReason::Shutdown, {}, preserve); }

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

  void expectPreparation() {
    testing::InSequence sequence;
    DesiredDriveState state;
    state.up = true;
    preparationComplete = EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(state));
    if (empty) {
      preparationComplete = EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Up, _));
    }
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

// Eligibility is based on the pre-registration record, including the operator's intent.
TEST_F(DriveControllerTest, StartupRecoveryPreservesKnownVidAcrossStatusPublication) {
  for (const auto status : AllDriveStatuses) {
    for (const bool desiredUp : {false, true}) {
      SCOPED_TRACE(toString(status) + (desiredUp ? " desired up" : " desired down"));
      previousDrive.emplace();
      previousDrive->driveStatus = status;
      previousDrive->desiredUp = desiredUp;
      previousDrive->currentVid = status == DriveStatus::Up ? std::nullopt : std::make_optional<std::string>("V00001");
      const auto expectedVid = previousDrive->currentVid;
      // Unknown and Shutdown do not establish that cleanup completed either.
      const bool recover = desiredUp;
      const auto previousCleanings = cleanings;
      EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
      if (!recover) {
        DesiredDriveState desired;
        desired.up = desiredUp;
        EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(desired));
        EXPECT_CALL(scheduler, createTapeDriveStatus(_, _, MountType::NoMount, DriveStatus::Down, _, _))
          .WillOnce(
            Invoke([&](const auto&, const DesiredDriveState& registered, const auto&, const auto&, const auto&, auto&) {
              EXPECT_GT(stateReads, 0);
              EXPECT_FALSE(registered.up);
            }));
      }
      EXPECT_CALL(scheduler, reportSchedulerBackendName("drive", _));
      if (recover) {
        EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _))
          .WillOnce(Invoke([&](const auto&, auto, auto, auto&) {
            EXPECT_GT(stateReads, 0);
            EXPECT_EQ(previousCleanings, cleanings);
            previousDrive->currentVid.reset();
          }));
      }
      ASSERT_TRUE(registerDrive());
      EXPECT_TRUE(controller->isReady());
      EXPECT_EQ(previousCleanings, cleanings);
      if (recover) {
        testing::InSequence preparationSequence;
        DesiredDriveState up;
        up.up = true;
        EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
        EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _));
        EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
        EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Up, _));
        ASSERT_TRUE(prepare());
        EXPECT_EQ(previousCleanings + 1, cleanings);
        EXPECT_EQ(expectedVid, cleanedVid);
        EXPECT_TRUE(waitedForMedia);
      }
      if (!recover) {
        testing::InSequence waitingSequence;
        EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
        EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
        EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Throw(std::runtime_error("end wait")));
        EXPECT_THROW(prepare(), std::runtime_error);
        EXPECT_EQ(0, probes);
        EXPECT_EQ(previousCleanings, cleanings);
        EXPECT_EQ(0, transfers);
        EXPECT_EQ(0, schedules);
        sleeps.clear();
      }
      ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&scheduler));
    }
  }
}

// Verify a failed recovery status report aborts startup without touching the drive.
TEST_F(DriveControllerTest, RecoveryStatusPublicationFailureDoesNotTouchHardware) {
  testing::InSequence sequence;
  previousDrive.emplace();
  previousDrive->driveStatus = DriveStatus::Transferring;
  previousDrive->desiredUp = true;
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _))
    .WillOnce(Throw(std::runtime_error("publication failed")));
  EXPECT_EQ(1, controller->run());
  EXPECT_FALSE(controller->isReady());
  EXPECT_EQ(0, cleanings);
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
}

// Verify an operator down request delays cleanup until a subsequent up request.
TEST_F(DriveControllerTest, OperatorDownBeforeRecoveryDefersCleaning) {
  previousDrive.emplace();
  previousDrive->driveStatus = DriveStatus::Transferring;
  previousDrive->desiredUp = true;
  previousDrive->currentVid = "V00001";
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _))
    .WillOnce(Invoke([&](const auto&, auto, auto, auto&) { previousDrive->currentVid.reset(); }));
  EXPECT_CALL(scheduler, reportSchedulerBackendName("drive", _));
  ASSERT_TRUE(registerDrive());

  testing::InSequence sequence;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _))
    .WillOnce(Invoke([&](const auto&, auto, auto, auto&) { EXPECT_EQ(0, cleanings); }));
  DesiredDriveState up;
  up.up = true;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Up, _));
  clean = [&] {
    EXPECT_EQ("V00001", cleanedVid);
    EXPECT_EQ(1, sleeps.size());
    EXPECT_EQ(0, probes);
    return true;
  };
  prepare();
  EXPECT_EQ(1, cleanings);
  EXPECT_EQ(0, schedules);
}

// Verify a down request during cleanup prevents scheduling.
TEST_F(DriveControllerTest, RecoveryRespectsOperatorDownDuringCleaning) {
  previousDrive.emplace();
  previousDrive->driveStatus = DriveStatus::Mounting;
  previousDrive->desiredUp = true;
  previousDrive->currentVid = "V00001";
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _));
  EXPECT_CALL(scheduler, reportSchedulerBackendName("drive", _));
  ASSERT_TRUE(registerDrive());

  DesiredDriveState state;
  state.up = true;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).Times(2).WillRepeatedly(Invoke([&](const auto&, auto&) {
    return state;
  }));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  clean = [&] {
    state.up = false;
    state.reason = "Operator maintenance";
    return true;
  };
  prepare();
  EXPECT_EQ(1, cleanings);
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
  EXPECT_FALSE(state.up);
  EXPECT_EQ("Operator maintenance", state.reason);
}

// Verify failed cleanup preserves its reason and requires another up request before retrying.
TEST_F(DriveControllerTest, CleaningFailureRequiresAnotherUpRequestAndPreservesReason) {
  DesiredDriveState up;
  up.up = true;
  DesiredDriveState failed;
  failed.reason = "Specific cleaner failure";
  testing::InSequence sequence;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(failed));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& state, auto&) {
      EXPECT_FALSE(state.up);
      EXPECT_FALSE(state.reason.has_value());
    }));
  clean = [] { return false; };
  prepare();
  EXPECT_EQ(1, cleanings);
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);

  // No new hardware access while down. An explicit up retries cleaning with no stale VID.
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(failed));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Up, _));
  clean = [&] {
    EXPECT_EQ(1, sleeps.size());
    EXPECT_EQ(0, probes);
    return true;
  };
  prepare();
  EXPECT_EQ(2, cleanings);
  EXPECT_FALSE(cleanedVid.has_value());
  EXPECT_EQ(0, schedules);
}

// Verify failed cleanup retains available exception details and keeps the drive down.
TEST_F(DriveControllerTest, CleaningExceptionsKeepDriveDown) {
  for (const int failure : {0, 1, 2, 3}) {
    SCOPED_TRACE(failure);
    DesiredDriveState up;
    up.up = true;
    testing::InSequence sequence;
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
    EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _));
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
    EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
    EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
      .WillOnce(Invoke([failure](const auto&, const DesiredDriveState& state, auto&) {
        EXPECT_FALSE(state.up);
        EXPECT_EQ(formatDriveDownReason(DriveDownReason::DriveCleanupFailed,
                                        failure == 3 ? "Unknown exception during drive cleanup" :
                                        failure == 0 ? "" :
                                                       "cleaner failed"),
                  state.reason);
      }));
    clean = [failure]() -> bool {
      if (failure == 0) {
        return false;
      }
      if (failure == 2) {
        throw cta::exception::Exception("cleaner failed");
      }
      if (failure == 3) {
        throw 42;
      }
      throw std::runtime_error("cleaner failed");
    };
    prepare();
    EXPECT_EQ(0, probes);
    EXPECT_EQ(0, schedules);
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&scheduler));
  }
}

// Verify one up transition cleans once before scheduling and ordinary retries do not clean again.
TEST_F(DriveControllerTest, UpTransitionCleansOnlyOnceBeforeScheduling) {
  testing::InSequence sequence;
  expectTransition();
  ASSERT_TRUE(prepare());
  expectPreparation();
  EXPECT_TRUE(iteration());
  expectPreparation();
  EXPECT_TRUE(iteration());
  EXPECT_EQ(1, cleanings);
  EXPECT_EQ(0, probes);
  EXPECT_EQ(2, schedules);
}

// Transition cleanup establishes readiness without a separate diagnostic probe.
TEST_F(DriveControllerTest, TransitionCleansWithoutDiagnosticProbe) {
  empty = false;
  clean = [&] {
    empty = true;
    return true;
  };
  expectTransition();
  EXPECT_TRUE(prepare());
  EXPECT_TRUE(empty);
  EXPECT_EQ(1, cleanings);
  EXPECT_EQ(0, probes);
}

// Reusable sessions preserve the empty-drive guarantee.
TEST_F(DriveControllerTest, ReusableSessionsNeedNoProbeOrAdditionalCleaning) {
  probe = [] { ADD_FAILURE() << "Controller must not probe the drive"; };
  expectTransition();
  ASSERT_TRUE(prepare());
  supplyMount();
  for (int i = 0; i < 2; ++i) {
    expectPreparation();
    EXPECT_TRUE(iteration());
  }
  EXPECT_EQ(1, cleanings);
  EXPECT_EQ(2, transfers);
  EXPECT_EQ(0, probes);
}

// If the drive name is owned by another host or logical library, registration fails.
TEST_F(DriveControllerTest, RegistrationConflictStopsStartup) {
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(false));
  EXPECT_EQ(1, controller->run());
  EXPECT_FALSE(controller->isReady());
  EXPECT_EQ(0, schedules);
}

// If the drive already has an operator reason and comment, registration preserves both.
TEST_F(DriveControllerTest, RegistrationPreservesOperatorReasonAndComment) {
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
TEST_F(DriveControllerTest, RegistrationPreservesNewUpRequestAndPublishesReadinessLast) {
  DesiredDriveState state;
  state.up = true;
  state.reason = "Setting drive up";
  state.comment = "Operator comment";
  EXPECT_FALSE(controller->isReady());
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(state));
  EXPECT_CALL(scheduler, createTapeDriveStatus(_, _, MountType::NoMount, DriveStatus::Down, _, _))
    .WillOnce(Invoke([&](const auto&, const DesiredDriveState& desired, const auto&, const auto&, const auto&, auto&) {
      EXPECT_FALSE(controller->isReady());
      EXPECT_TRUE(desired.up);
      EXPECT_EQ(state.reason, desired.reason);
      EXPECT_EQ(state.comment, desired.comment);
    }));
  EXPECT_CALL(scheduler, reportSchedulerBackendName("drive", _)).WillOnce(Invoke([&](const auto&, auto&) {
    EXPECT_FALSE(controller->isReady());
  }));
  EXPECT_TRUE(registerDrive());
  EXPECT_TRUE(controller->isReady());
}

TEST_F(DriveControllerTest, ShutdownDoesNotPreserveUpReason) {
  DesiredDriveState state;
  state.up = true;
  state.reason = "Setting drive up";
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(state));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& desired, auto&) {
      EXPECT_FALSE(desired.up);
      EXPECT_EQ(formatDriveDownReason(DriveDownReason::Shutdown), desired.reason);
    }));
  down(true);
}

// If the drive has no down reason or was cleanly shut down, drive registration replaces that state with
TEST_F(DriveControllerTest, RegistrationReplacesAbsentAndCleanShutdownReasonsWithStartup) {
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
TEST_F(DriveControllerTest, MissingDriveEndsRunWithoutRegistrationOrDownPublication) {
  testing::InSequence sequence;
  expectRunStartup();
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Throw(Scheduler::NoSuchDrive("missing")));

  EXPECT_EQ(1, controller->run());
  EXPECT_FALSE(controller->isReady());
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, cleanings);
  EXPECT_EQ(0, schedules);
  EXPECT_TRUE(sleeps.empty());
  EXPECT_THAT(logger.getLog(), testing::HasSubstr("Drive is missing from the catalogue. Exiting."));
}

TEST_F(DriveControllerTest, MissingDrivePropagatesFromWaitForUp) {
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Throw(Scheduler::NoSuchDrive("missing")));
  EXPECT_THROW(waitForUp(), Scheduler::NoSuchDrive);
}

// If publishing the drive registration fails, run should end with a failure result.
TEST_F(DriveControllerTest, RegistrationPublicationFailureAbortsStartupBeforeLibraryOrScheduling) {
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

  EXPECT_EQ(1, controller->run());
  EXPECT_FALSE(controller->isReady());
  EXPECT_EQ(0, libraryChecks);
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
}

// If publishing the scheduler backend name fails, run should end with a failure result.
TEST_F(DriveControllerTest, SchedulerBackendPublicationFailureAbortsStartupBeforeLibraryOrScheduling) {
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

  EXPECT_EQ(1, controller->run());
  EXPECT_FALSE(controller->isReady());
  EXPECT_EQ(0, libraryChecks);
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
}

// If registration loses its database connection, run should end with a failure result.
TEST_F(DriveControllerTest, StartupDatabaseFailureAbortsBeforeLibraryOrScheduling) {
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

  EXPECT_EQ(1, controller->run());
  EXPECT_FALSE(controller->isReady());
  EXPECT_EQ(0, libraryChecks);
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
}

// If the configured logical library is absent, startup waits for it to appear.
TEST_F(DriveControllerTest, MissingLogicalLibraryWaitsUntilAvailable) {
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
TEST_F(DriveControllerTest, LogicalLibraryDatabaseFailurePropagatesWithoutWaiting) {
  libraryExists = []() -> bool { throw exception::LostDatabaseConnection("database unavailable"); };
  EXPECT_THROW(waitForLibrary(), exception::LostDatabaseConnection);
  EXPECT_TRUE(sleeps.empty());
}

// If the logical-library lookup loses its database connection, run should end with a failure result.
TEST_F(DriveControllerTest, LogicalLibraryDatabaseFailureEndsStartupWithoutScheduling) {
  testing::InSequence sequence;
  libraryExists = []() -> bool { throw exception::LostDatabaseConnection("objectstore unavailable"); };
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, createTapeDriveStatus(_, _, MountType::NoMount, DriveStatus::Down, _, _));
  EXPECT_CALL(scheduler, reportSchedulerBackendName("drive", _));

  expectShutdown();
  EXPECT_EQ(1, controller->run());
  EXPECT_FALSE(controller->isReady());
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
}

// Only failures after successful registration publish down on exit.
TEST_F(DriveControllerTest, UnknownStartupFailuresPublishDownOnlyAfterRegistration) {
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
    EXPECT_EQ(1, controller->run());
    EXPECT_FALSE(controller->isReady());
    EXPECT_EQ(0, probes);
    EXPECT_EQ(0, cleanings);
    EXPECT_EQ(0, transfers);
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&scheduler));
  }
}

TEST_F(DriveControllerTest, UnknownOwnershipFailureDoesNotPublishOrTouchHardware) {
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Throw(42));
  EXPECT_EQ(1, controller->run());
  EXPECT_FALSE(controller->isReady());
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, cleanings);
  EXPECT_EQ(0, transfers);
}

TEST_F(DriveControllerTest, DownWaitingAndExitNeverTouchHardware) {
  testing::InSequence sequence;
  expectRunStartup();
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Throw(std::runtime_error("database unavailable")));
  expectShutdown();
  EXPECT_EQ(1, controller->run());
  EXPECT_FALSE(controller->isReady());
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, cleanings);
  EXPECT_EQ(0, schedules);
  EXPECT_EQ(0, transfers);
  EXPECT_EQ(2, sleeps.size());
}

TEST_F(DriveControllerTest, CleaningTransitionPublicationPrecedesAllHardwareAccess) {
  DesiredDriveState up;
  up.up = true;
  bool preparing = false;
  testing::InSequence sequence;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _))
    .WillOnce(Invoke([&](const auto&, auto, auto, auto&) {
      EXPECT_EQ(0, probes);
      EXPECT_EQ(0, cleanings);
      preparing = true;
    }));
  probe = [&] { EXPECT_TRUE(preparing); };
  clean = [&] {
    EXPECT_TRUE(preparing);
    return true;
  };
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Up, _));
  prepare();
  EXPECT_EQ(0, probes);
  EXPECT_EQ(1, cleanings);
}

// The catalogue can acknowledge a session's down request before the controller observes it.
TEST_F(DriveControllerTest, ReportedDownWithNewUpRequestRequiresCleaning) {
  previousDrive->driveStatus = DriveStatus::Down;
  previousDrive->desiredUp = true;
  previousDrive->currentVid = "V00001";
  DesiredDriveState up;
  up.up = true;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_FALSE(iteration());
  EXPECT_EQ(0, schedules);
  EXPECT_EQ(0, cleanings);
  EXPECT_EQ(0, probes);
  expectTransition();
  ASSERT_TRUE(prepare());
  EXPECT_EQ("V00001", cleanedVid);
  EXPECT_EQ(1, cleanings);
}

TEST_F(DriveControllerTest, FailedPreparationTransitionDoesNotTouchHardware) {
  DesiredDriveState up;
  up.up = true;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _))
    .WillOnce(Throw(std::runtime_error("publication failed")));
  EXPECT_THROW(prepare(), std::runtime_error);
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, cleanings);
  EXPECT_EQ(0, schedules);
}

// Drive preparation and scheduling.

// If the desired-state lookup loses its database connection, the iteration propagates the error.
TEST_F(DriveControllerTest, DesiredStateDatabaseFailurePropagates) {
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _))
    .WillOnce(Throw(exception::LostDatabaseConnection("database unavailable")));
  EXPECT_THROW(iteration(), exception::LostDatabaseConnection);
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
}

// If the desired state remains down, the controller refreshes its reported status and waits.
TEST_F(DriveControllerTest, DownDriveWaitsForOperatorUpBeforeCleaning) {
  testing::InSequence sequence;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  DesiredDriveState up;
  up.up = true;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Up, _));
  prepare();
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
  EXPECT_THAT(sleeps, testing::ElementsAre(config.mounts.drive_state_poll_interval_secs));
}

// If publishing Up fails before scheduling, the controller must not schedule work.
TEST_F(DriveControllerTest, UpPublicationFailurePreventsScheduling) {
  DesiredDriveState up;
  up.up = true;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Up, _))
    .WillOnce(Throw(std::runtime_error("publication failed")));
  EXPECT_THROW(iteration(), std::runtime_error);
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
}

// A down request ends the current up period.
TEST_F(DriveControllerTest, DownWaitingDoesNotCaptureCatalogueVid) {
  testing::InSequence sequence;
  previousDrive->currentVid = "STALE";
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _))
    .WillOnce(Invoke([&](const auto&, auto, auto, auto&) {
      EXPECT_EQ(0, stateReads);
      EXPECT_EQ(0, cleanings);
      EXPECT_EQ(0, probes);
      previousDrive->currentVid.reset();
    }));
  expectTransition();
  ASSERT_TRUE(prepare());
  EXPECT_FALSE(cleanedVid.has_value());
  EXPECT_EQ(1, cleanings);
  EXPECT_THAT(sleeps, testing::ElementsAre(config.mounts.drive_state_poll_interval_secs));
}

TEST_F(DriveControllerTest, DesiredDownEndsUpPeriodWithoutHardwareAccess) {
  previousDrive->currentVid = "STALE";
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_FALSE(iteration());
  EXPECT_EQ(0, cleanings);
  EXPECT_EQ(0, schedules);
  EXPECT_EQ(0, probes);

  // A stale catalogue VID from the down transition must not become recovery state.
  previousDrive->currentVid.reset();
  expectTransition();
  ASSERT_TRUE(prepare());
  EXPECT_FALSE(cleanedVid.has_value());
}

// A missing catalogue record prevents further scheduling.
TEST_F(DriveControllerTest, MissingCatalogueEntryEndsScheduling) {
  DesiredDriveState up;
  up.up = true;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  previousDrive.reset();
  EXPECT_THROW(iteration(), Scheduler::NoSuchDrive);
  EXPECT_EQ(0, cleanings);
  EXPECT_EQ(0, schedules);
}

// If scheduling returns no mount, the controller waits before the next attempt.
TEST_F(DriveControllerTest, IdleMountWaitsAndRechecksDriveBeforeRetry) {
  expectPreparation();
  iteration();
  expectPreparation();
  iteration();
  EXPECT_EQ(0, probes);
  EXPECT_EQ(2, schedules);
  EXPECT_EQ(0, transfers);
  EXPECT_FALSE(iterationResult.successful);
  EXPECT_TRUE(sleeps.empty());
}

// Each observed down/up period requires one new cleanup.
TEST_F(DriveControllerTest, OperatorDownUpStartsAnotherCleanedUpPeriod) {
  testing::InSequence sequence;
  expectRunStartup();
  expectTransition();
  expectPreparation();
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  expectTransition();
  expectPreparation();
  expectShutdown();
  onSleep = [&] {
    if (schedules == 2) {
      controller->stop();
    }
  };
  EXPECT_EQ(0, controller->run());
  EXPECT_EQ(2, cleanings);
  EXPECT_EQ(2, schedules);
  EXPECT_EQ(0, probes);
}

// If mount scheduling times out, the controller waits and allows a later iteration.
TEST_F(DriveControllerTest, SchedulingTimeoutWaitsAndAllowsAnotherIteration) {
  schedule = []() -> std::unique_ptr<TapeMount> { throw exception::TimeoutException("timeout"); };
  expectPreparation();
  EXPECT_NO_THROW(iteration());
  schedule = {};
  expectPreparation();
  iteration();
  EXPECT_EQ(0, probes);
  EXPECT_EQ(2, schedules);
  EXPECT_FALSE(iterationResult.successful);
  EXPECT_TRUE(sleeps.empty());
}

// Database failures during scheduling use the ordinary retry delay.
TEST_F(DriveControllerTest, SchedulingDatabaseFailureWaitsAndAllowsAnotherIteration) {
  schedule = []() -> std::unique_ptr<TapeMount> { throw exception::LostDatabaseConnection("database unavailable"); };
  expectPreparation();
  EXPECT_CALL(scheduler, ping(_)).Times(0);

  EXPECT_NO_THROW(iteration());
  EXPECT_THAT(logger.getLog(), testing::HasSubstr("LVL=\"ERROR\""));
  EXPECT_EQ(nullptr, liveMount());

  schedule = {};
  expectPreparation();
  iteration();
  EXPECT_EQ(0, probes);
  EXPECT_EQ(2, schedules);
  EXPECT_FALSE(iterationResult.successful);
  EXPECT_TRUE(sleeps.empty());
}

// Unexpected scheduling failures are logged and retried after the idle delay without cleaning.
TEST_F(DriveControllerTest, UnexpectedSchedulingFailureWaitsAndAllowsAnotherIteration) {
  schedule = []() -> std::unique_ptr<TapeMount> { throw std::runtime_error("unexpected scheduler failure"); };
  unsigned int cleanAttempts = 0;
  clean = [&] {
    ++cleanAttempts;
    return true;
  };
  expectPreparation();

  EXPECT_NO_THROW(iteration());
  EXPECT_THAT(logger.getLog(), testing::HasSubstr("Scheduling failed unexpectedly"));
  EXPECT_THAT(logger.getLog(), testing::HasSubstr("LVL=\"ERROR\""));
  EXPECT_EQ(nullptr, liveMount());
  EXPECT_EQ(0, cleanAttempts);
  EXPECT_EQ(0, transfers);
  EXPECT_TRUE(sleeps.empty());

  supplyMount();
  expectPreparation();
  EXPECT_NO_THROW(iteration());
  EXPECT_EQ(0, probes);
  EXPECT_EQ(2, schedules);
  EXPECT_EQ(1, transfers);
  EXPECT_EQ(1, destroyed);
  EXPECT_EQ(nullptr, liveMount());
  EXPECT_EQ(0, cleanAttempts);
  EXPECT_TRUE(sleeps.empty());
}

// Scheduling without work does not start a TapeSession.
TEST_F(DriveControllerTest, IdleDriveDoesNotStartATapeSession) {
  expectPreparation();

  iteration();

  EXPECT_EQ(0, transfers);
  EXPECT_EQ(0, schedulerResets);
  EXPECT_EQ(nullptr, liveMount());
}

TEST_F(DriveControllerTest, SchedulerRetiredAfterMountDestructionAndBeforeDelay) {
  testing::InSequence sequence;
  expectRunStartup();
  expectTransition();
  expectPreparation();
  expectPreparation();
  expectShutdown();
  supplyMount();
  transfer = [&](TapeMount&) { return TapeSessionResult {.successful = transfers == 1}; };
  onSchedulerReset = [&] { EXPECT_EQ(transfers, destroyed); };
  onSleep = [&] {
    EXPECT_EQ(2, schedulerResets);
    EXPECT_EQ(2, destroyed);
    controller->stop();
  };
  EXPECT_EQ(0, controller->run());
  EXPECT_EQ(2, schedulerResets);
  EXPECT_THAT(sleeps, testing::ElementsAre(config.mounts.idle_scheduling_interval_secs));
}

TEST_F(DriveControllerTest, IdlePollResetsBeforeRetryDelay) {
  testing::InSequence sequence;
  expectRunStartup();
  expectTransition();
  expectPreparation();
  expectShutdown();
  onSleep = [&] {
    EXPECT_EQ(1, schedulerResets);
    controller->stop();
  };
  EXPECT_EQ(0, controller->run());
  EXPECT_EQ(1, schedulerResets);
  EXPECT_EQ(0, transfers);
}

TEST_F(DriveControllerTest, UnusableSessionResetsBeforeLeavingUpPeriod) {
  testing::InSequence sequence;
  expectRunStartup();
  expectTransition();
  expectPreparation();
  expectShutdown();  // The session result requests down.
  expectShutdown();  // The stop request then shuts down the controller.
  supplyMount();
  transfer = [&](TapeMount&) {
    controller->stop();
    return TapeSessionResult {.driveReusable = false};
  };
  onSchedulerReset = [&] { EXPECT_EQ(1, destroyed); };
  EXPECT_EQ(0, controller->run());
  EXPECT_EQ(1, schedulerResets);
  EXPECT_TRUE(sleeps.empty());
}

TEST_F(DriveControllerTest, IdlePollResetFailureExitsWithoutRetrySleep) {
  testing::InSequence sequence;
  expectRunStartup();
  expectTransition();
  expectPreparation();
  expectShutdown();
  onSchedulerReset = [] { throw std::runtime_error("scheduler replacement failed"); };
  EXPECT_EQ(1, controller->run());
  EXPECT_EQ(1, schedules);
  EXPECT_EQ(0, transfers);
  EXPECT_EQ(1, schedulerResets);
  EXPECT_TRUE(sleeps.empty());
}

TEST_F(DriveControllerTest, SchedulingFailureResetFailurePropagatesWithoutRetrySleep) {
  for (const bool timeout : {false, true}) {
    SCOPED_TRACE(timeout);
    testing::InSequence sequence;
    expectRunStartup();
    expectTransition();
    expectPreparation();
    expectShutdown();
    schedule = [timeout]() -> std::unique_ptr<TapeMount> {
      if (timeout) {
        throw exception::TimeoutException("timeout");
      }
      throw std::runtime_error("scheduling failed");
    };
    onSchedulerReset = [] { throw std::runtime_error("scheduler replacement failed"); };
    EXPECT_EQ(1, controller->run());
    EXPECT_EQ(0, transfers);
    EXPECT_TRUE(sleeps.empty());
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&scheduler));
  }
  EXPECT_EQ(2, schedulerResets);
}

TEST_F(DriveControllerTest, SchedulerResetFailurePublishesDownAndExits) {
  testing::InSequence sequence;
  expectRunStartup();
  DesiredDriveState up;
  up.up = true;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _));
  expectPreparation();
  expectPreparation();
  supplyMount();
  onSchedulerReset = [] { throw std::runtime_error("scheduler replacement failed"); };
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _));

  EXPECT_EQ(1, controller->run());
  EXPECT_EQ(1, schedulerResets);
  EXPECT_EQ(1, destroyed);
  EXPECT_FALSE(controller->isReady());
  EXPECT_TRUE(sleeps.empty());
}

// Transfers and recovery.

// A reusable drive without a recovery request remains available for scheduling.
TEST_F(DriveControllerTest, ReusableDriveWithoutRecoveryDoesNotRequestDown) {
  supplyMount();
  transfer = [](TapeMount&) {
    TapeSessionResult result;
    result.successful = true;
    return result;
  };
  expectPreparation();
  iteration();
  EXPECT_EQ(1, transfers);
  EXPECT_EQ(1, destroyed);
  EXPECT_TRUE(sleeps.empty());
}

// Each session receives its own live mount, released before scheduling again.
TEST_F(DriveControllerTest, SuccessiveSessionsReceiveTheirOwnMount) {
  supplyMount();
  transfer = [&](TapeMount& tapeMount) {
    EXPECT_EQ(&tapeMount, liveMount());
    return TapeSessionResult {};
  };

  expectPreparation();
  iteration();
  EXPECT_EQ(nullptr, liveMount());
  expectPreparation();
  iteration();

  EXPECT_EQ(2, transfers);
  EXPECT_EQ(0, probes);
  EXPECT_EQ(2, destroyed);
  EXPECT_EQ(nullptr, liveMount());
}

// A handled finalization failure has already completed the session's local cleanup.
TEST_F(DriveControllerTest, HandledSessionFailureRetriesWithoutCleaning) {
  supplyMount();
  transfer = [](TapeMount&) {
    TapeSessionResult result;
    result.successful = false;
    return result;
  };
  clean = [] {
    ADD_FAILURE() << "Handled session failures must not trigger another cleanup";
    return false;
  };
  expectPreparation();

  EXPECT_NO_THROW(iteration());
  EXPECT_EQ(1, destroyed);
  EXPECT_EQ(nullptr, liveMount());
  EXPECT_FALSE(iterationResult.successful);
  EXPECT_TRUE(sleeps.empty());
}

// File failures and informational events are paced using the same final outcome as the session log.
TEST_F(DriveControllerTest, SessionOutcomeDeterminesSchedulingDelay) {
  for (const bool fileFailed : {false, true}) {
    TapeSessionTracker tracker;
    tracker.beginTapeSession();
    if (fileFailed) {
      tracker.recordFailure(TapeSessionFailure::DiskRead);
    } else {
      tracker.recordEvent(TapeSessionEvent::DiskSpaceReservationTestFailure);
    }
    tracker.reportState(cta::tape::session::TapeSessionState::Finished);
    supplyMount();
    transfer = [&](TapeMount&) {
      TapeSessionResult result;
      result.successful = !tracker.hasFailures();
      return result;
    };
    sleeps.clear();
    expectPreparation();
    iteration();
    EXPECT_EQ(!fileFailed, iterationResult.successful);
    EXPECT_TRUE(sleeps.empty());
  }
}

// Exceptions with completed worker teardown can recover through controller cleanup.
TEST_F(DriveControllerTest, SessionExceptionsCleanAndPermitRetry) {
  for (const auto failure : {"standard", "database", "unknown"}) {
    SCOPED_TRACE(failure);
    testing::InSequence sequence;
    expectPreparation();
    expectTransition();  // Recovery checks up intent before claiming the drive.
    supplyMount();
    transfer = [failure](TapeMount&) -> TapeSessionResult {
      if (std::string(failure) == "database") {
        throw exception::LostDatabaseConnection("database unavailable");
      }
      if (std::string(failure) == "standard") {
        throw std::runtime_error("session failed");
      }
      throw 42;
    };
    previousDrive->currentVid = "STALE";
    const auto oldDestroyed = destroyed;
    clean = [&] {
      EXPECT_NE(nullptr, liveMount());
      EXPECT_EQ(oldDestroyed, destroyed);
      EXPECT_EQ("V00001", cleanedVid);
      return true;
    };
    EXPECT_TRUE(iteration());
    EXPECT_EQ(oldDestroyed + 1, destroyed);
    EXPECT_EQ(nullptr, liveMount());
    EXPECT_FALSE(iterationResult.successful);
    EXPECT_TRUE(sleeps.empty());
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&scheduler));
  }
  EXPECT_EQ(3, cleanings);
  EXPECT_EQ(0, schedulerResets);
}

// Recovery must return to the inner scheduling loop, not repeat transition cleanup.
TEST_F(DriveControllerTest, RecoveredSessionSchedulesAgainWithoutAnotherTransition) {
  testing::InSequence sequence;
  expectRunStartup();
  expectTransition();
  expectPreparation();
  expectTransition();
  expectPreparation();
  expectShutdown();
  supplyMount();
  transfer = [&](TapeMount&) -> TapeSessionResult {
    if (transfers == 1) {
      throw std::runtime_error("session failed");
    }
    controller->stop();
    return {};
  };
  EXPECT_EQ(0, controller->run());
  EXPECT_EQ(2, transfers);
  EXPECT_EQ(2, cleanings);  // Startup and exception recovery only.
  EXPECT_EQ(2, destroyed);
  EXPECT_EQ(2, schedulerResets);
  EXPECT_THAT(sleeps, testing::ElementsAre(config.mounts.idle_scheduling_interval_secs));
}

TEST_F(DriveControllerTest, MissingDriveBeforeCleanupDoesNotTouchHardware) {
  DesiredDriveState up;
  up.up = true;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  previousDrive.reset();
  EXPECT_THROW(prepare(), Scheduler::NoSuchDrive);
  EXPECT_EQ(0, cleanings);
}

TEST_F(DriveControllerTest, SessionRecoveryFailureEndsUpPeriod) {
  for (const int failure : {0, 1, 2}) {
    SCOPED_TRACE(failure);
    testing::InSequence sequence;
    expectPreparation();
    DesiredDriveState up;
    up.up = true;
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
    EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _));
    DesiredDriveState down;
    down.reason = "Specific ejection failure";
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(down));
    EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
    EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
      .WillOnce(Invoke([](const auto&, const DesiredDriveState& desired, auto&) {
        EXPECT_FALSE(desired.up);
        EXPECT_FALSE(desired.reason);
      }));
    supplyMount();
    transfer = [](TapeMount&) -> TapeSessionResult { throw 42; };
    clean = [failure]() -> bool {
      if (failure == 1) {
        throw std::runtime_error("cleaner failed");
      }
      if (failure == 2) {
        throw 43;
      }
      return false;
    };
    EXPECT_FALSE(iteration());
    EXPECT_TRUE(sleeps.empty());
    EXPECT_EQ(nullptr, liveMount());
    EXPECT_EQ(failure + 1, cleanings);
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&scheduler));
  }
}

TEST_F(DriveControllerTest, SessionExceptionWhileDownDefersCleanupAndPreservesVid) {
  testing::InSequence sequence;
  expectPreparation();
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  supplyMount();
  transfer = [](TapeMount&) -> TapeSessionResult { throw std::runtime_error("session failed"); };
  EXPECT_FALSE(iteration());
  EXPECT_EQ(0, cleanings);
  EXPECT_TRUE(sleeps.empty());
  EXPECT_EQ(1, destroyed);
  previousDrive->currentVid = "STALE";
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _))
    .WillOnce(Invoke([&](const auto&, auto, auto, auto&) { previousDrive->currentVid.reset(); }));
  expectTransition();
  ASSERT_TRUE(prepare());
  EXPECT_EQ("V00001", cleanedVid);
}

TEST_F(DriveControllerTest, OperatorDownDuringSessionRecoveryPreventsRetry) {
  testing::InSequence sequence;
  expectPreparation();
  DesiredDriveState up;
  up.up = true;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _));
  DesiredDriveState down;
  down.reason = "Operator maintenance";
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(down));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  supplyMount();
  transfer = [](TapeMount&) -> TapeSessionResult { throw 42; };
  EXPECT_FALSE(iteration());
  EXPECT_EQ(1, cleanings);
  EXPECT_EQ(1, schedules);
  EXPECT_TRUE(sleeps.empty());
}

TEST_F(DriveControllerTest, StopDuringFailedSessionStillRecoversWithoutRetry) {
  testing::InSequence sequence;
  expectRunStartup();
  expectTransition();
  expectPreparation();
  expectTransition();
  expectShutdown();
  supplyMount();
  transfer = [&](TapeMount&) -> TapeSessionResult {
    controller->stop();
    throw std::runtime_error("session failed while stopping");
  };
  EXPECT_EQ(0, controller->run());
  EXPECT_EQ(2, cleanings);
  EXPECT_EQ(1, schedules);
  EXPECT_EQ(1, destroyed);
  EXPECT_TRUE(sleeps.empty());
}

TEST_F(DriveControllerTest, IncompleteWorkerTeardownPreservesOriginalException) {
  expectPreparation();
  supplyMount();
  transfer = [](TapeMount&) -> TapeSessionResult {
    try {
      throw std::runtime_error("worker start failed");
    } catch (...) {
      std::throw_with_nested(TapeSessionWorkerTeardownIncomplete());
    }
  };
  try {
    iteration();
    FAIL() << "Expected fatal worker teardown failure";
  } catch (const TapeSessionWorkerTeardownIncomplete& ex) {
    try {
      std::rethrow_if_nested(ex);
      FAIL() << "Expected original worker failure";
    } catch (const std::runtime_error& cause) {
      EXPECT_STREQ("worker start failed", cause.what());
    }
  }
  EXPECT_EQ(0, cleanings);
  EXPECT_EQ(0, schedulerResets);
  EXPECT_EQ(1, destroyed);
}

// If a transfer reports an unusable drive with a specific reason, the controller requests it down.
TEST_F(DriveControllerTest, UnusableDriveRequestsDownAndPreservesSpecificReason) {
  bool downPublished = false;
  supplyMount();
  transfer = [](TapeMount&) {
    TapeSessionResult result;
    result.driveReusable = false;
    result.successful = false;
    return result;
  };
  expectPreparation();
  DesiredDriveState state;
  state.reason = "Unload failed";
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).After(preparationComplete).WillOnce(Return(state));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([&](const auto&, const DesiredDriveState& desired, auto&) {
      downPublished = true;
      EXPECT_FALSE(desired.up);
      EXPECT_FALSE(desired.reason);
    }));
  iteration();
  EXPECT_EQ(1, destroyed);
  EXPECT_EQ(0, schedulerResets);
  EXPECT_TRUE(downPublished);
  EXPECT_TRUE(sleeps.empty());

  // Recover using the session VID even after its mount has been destroyed.
  schedule = {};
  DesiredDriveState up;
  up.up = true;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).Times(2).WillRepeatedly(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Up, _));
  ASSERT_TRUE(prepare());
  EXPECT_EQ("V00001", cleanedVid);

  // Successful ejection must not carry that VID into an unrelated cleanup.
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).Times(2).WillRepeatedly(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Up, _));
  ASSERT_TRUE(prepare());
  EXPECT_FALSE(cleanedVid.has_value());
}

// If a transfer marks the drive unusable without a specific reason, the controller requests it down.
TEST_F(DriveControllerTest, UnusableDriveWithoutSpecificReasonPublishesTransferFailure) {
  supplyMount();
  transfer = [](TapeMount&) {
    TapeSessionResult result;
    result.driveReusable = false;
    return result;
  };
  expectPreparation();
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _))
    .After(preparationComplete)
    .WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& state, auto&) {
      EXPECT_FALSE(state.up);
      EXPECT_EQ(formatDriveDownReason(DriveDownReason::SessionLeftDriveUnusable), state.reason);
    }));
  iteration();
  EXPECT_EQ(1, destroyed);
}

// If down-state publication fails after an unusable transfer, the error propagates.
TEST_F(DriveControllerTest, DownPublicationFailureAfterTransferStillReleasesMount) {
  supplyMount();
  transfer = [](TapeMount&) {
    TapeSessionResult result;
    result.driveReusable = false;
    return result;
  };
  expectPreparation();
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _))
    .After(preparationComplete)
    .WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _))
    .WillOnce(Throw(std::runtime_error("publication failed")));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _));
  EXPECT_THROW(iteration(), std::runtime_error);
  EXPECT_EQ(1, destroyed);
}

// Failed session cleanup relinquishes the drive until another explicit up request.
TEST_F(DriveControllerTest, UnusableSessionThenDownWaitAndShutdownNeverAccessHardware) {
  supplyMount();
  transfer = [](TapeMount&) {
    TapeSessionResult result;
    result.driveReusable = false;
    return result;
  };
  testing::InSequence sequence;
  expectPreparation();
  DesiredDriveState failed;
  failed.reason = "Tape ejection failed";
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(failed));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _));
  iteration();
  EXPECT_EQ(0, probes);
  EXPECT_EQ(1, transfers);
  EXPECT_EQ(0, cleanings);

  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(failed));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Throw(std::runtime_error("end wait")));
  EXPECT_THROW(waitForUp(), std::runtime_error);
  expectRunStartup();
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(failed));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& desired, auto&) {
      EXPECT_FALSE(desired.up);
      EXPECT_FALSE(desired.reason);
    }));
  EXPECT_EQ(0, shutdown());
  EXPECT_EQ(0, probes);
  EXPECT_EQ(1, schedules);
  EXPECT_EQ(1, transfers);
  EXPECT_EQ(0, cleanings);
}

// Down-state publication.

// If publishing the reported down status fails, the controller still tries the desired down state.
TEST_F(DriveControllerTest, DownPublicationsBothRunAndFirstExceptionIsPreserved) {
  testing::InSequence sequence;
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _))
    .WillOnce(Throw(std::runtime_error("reported state failed")));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _)).WillOnce(Throw(std::logic_error("desired state failed")));
  try {
    down();
    FAIL() << "Expected publication failure";
  } catch (const std::runtime_error& ex) {
    EXPECT_STREQ("reported state failed", ex.what());
  }
}

// If publishing the desired down state fails, the controller propagates the error.
TEST_F(DriveControllerTest, DesiredPublicationFailurePropagates) {
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _)).WillOnce(Throw(std::runtime_error("failed")));
  EXPECT_THROW(down(), std::runtime_error);
}

// If reading the existing down reason fails, the controller still attempts both down publications.
TEST_F(DriveControllerTest, ReasonLookupFailureStillAttemptsBothDownPublications) {
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Throw(std::runtime_error("lookup failed")));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& state, auto&) {
      EXPECT_FALSE(state.up);
      EXPECT_FALSE(state.reason);
    }));
  EXPECT_THROW(down(true), std::runtime_error);
}

// Shutdown.

// A failure while the drive is down publishes down without touching tape hardware.
TEST_F(DriveControllerTest, IterationExceptionPublishesDownWithoutCleaning) {
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

  EXPECT_EQ(1, controller->run());
  EXPECT_FALSE(controller->isReady());
  EXPECT_EQ(0, cleanings);
  EXPECT_EQ(0, probes);
  EXPECT_THAT(logger.getLog(), testing::HasSubstr("Drive controller failed. Publishing down state before exit."));
}

// Incomplete worker teardown is fatal and cannot be repaired by controller cleanup.
TEST_F(DriveControllerTest, IncompleteWorkerTeardownExitsWithoutRecovery) {
  testing::InSequence sequence;
  expectRunStartup();
  expectTransition();
  expectPreparation();
  expectShutdown();
  supplyMount();
  transfer = [](TapeMount&) -> TapeSessionResult {
    try {
      throw 42;
    } catch (...) {
      std::throw_with_nested(TapeSessionWorkerTeardownIncomplete());
    }
  };
  EXPECT_EQ(1, controller->run());
  EXPECT_EQ(1, cleanings);  // Only the initial transition; no recovery with live workers.
  EXPECT_EQ(1, destroyed);
  EXPECT_EQ(0, schedulerResets);
  EXPECT_EQ(1, transfers);
}

// Shutdown publishes down without invoking the cleaner.
TEST_F(DriveControllerTest, ShutdownPublishesDownWithoutCleaning) {
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
TEST_F(DriveControllerTest, ShutdownAttemptsBothDownPublicationsAfterFailure) {
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
TEST_F(DriveControllerTest, ShutdownPublicationFailureReturnsNonzero) {
  testing::InSequence sequence;
  expectRunStartup();
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _)).WillOnce(Throw(std::runtime_error("publication failed")));
  EXPECT_EQ(1, shutdown());
  EXPECT_EQ(0, cleanings);
}

// Unknown publication failures follow the same best-effort exit policy as standard exceptions.
TEST_F(DriveControllerTest, ShutdownContainsUnknownFailuresAndAttemptsBothPublications) {
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
TEST_F(DriveControllerTest, ShutdownReplacesStartupAndCleanReasonsButPreservesOperatorReason) {
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

TEST_F(DriveControllerTest, StopWhileWaitingForLibraryExitsWithoutHardwareAccess) {
  testing::InSequence sequence;
  expectRunStartup();
  expectShutdown();
  libraryExists = [] { return false; };
  onSleep = [&] { controller->stop(); };

  EXPECT_EQ(0, controller->run());
  EXPECT_FALSE(controller->isReady());
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, cleanings);
  EXPECT_EQ(0, schedules);
  EXPECT_THAT(sleeps, testing::ElementsAre(config.mounts.logical_library_poll_interval_secs));
}

TEST_F(DriveControllerTest, StopWhileWaitingForUpExitsWithoutHardwareAccess) {
  testing::InSequence sequence;
  expectRunStartup();
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  expectShutdown();
  onSleep = [&] { controller->stop(); };

  EXPECT_EQ(0, controller->run());
  EXPECT_FALSE(controller->isReady());
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, cleanings);
  EXPECT_EQ(0, schedules);
  EXPECT_THAT(sleeps, testing::ElementsAre(config.mounts.drive_state_poll_interval_secs));
}

TEST_F(DriveControllerTest, StopDuringIdleSleepPublishesDownAndExits) {
  testing::InSequence sequence;
  expectRunStartup();
  expectTransition();
  expectPreparation();
  expectShutdown();
  onSleep = [&] { controller->stop(); };

  EXPECT_EQ(0, controller->run());
  EXPECT_EQ(1, schedules);
  EXPECT_EQ(0, transfers);
  EXPECT_FALSE(controller->isReady());
}

TEST_F(DriveControllerTest, StopDuringSessionAllowsCompletionBeforeShutdown) {
  testing::InSequence sequence;
  expectRunStartup();
  expectTransition();
  expectPreparation();
  expectShutdown();
  supplyMount();
  bool completed = false;
  transfer = [&](TapeMount&) {
    controller->stop();
    EXPECT_NE(nullptr, liveMount());
    completed = true;
    return TapeSessionResult {};
  };

  EXPECT_EQ(0, controller->run());
  EXPECT_TRUE(completed);
  EXPECT_EQ(1, transfers);
  EXPECT_EQ(1, destroyed);
  EXPECT_FALSE(controller->isReady());
}

TEST_F(DriveControllerTest, StopStillReturnsFailureWhenShutdownPublicationFails) {
  testing::InSequence sequence;
  expectRunStartup();
  libraryExists = [&] {
    controller->stop();
    return true;
  };
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _))
    .WillOnce(Throw(std::runtime_error("publication failed")));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _));

  EXPECT_EQ(1, controller->run());
  EXPECT_FALSE(controller->isReady());
  EXPECT_EQ(0, probes);
}

TEST_F(DriveControllerTest, LivenessUsesEachStateTimeoutAtItsBoundary) {
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

TEST_F(DriveControllerTest, TransferProgressRefreshesOnlyInactivityAndNewSessionResetsIt) {
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

TEST_F(DriveControllerTest, ActiveTrackerIsOwnedAndClearedOnReturnAndException) {
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

TEST_F(DriveControllerTest, UnloadTimeoutDefaultsAndValidation) {
  EXPECT_EQ(900, config.mounts.tape_unload_timeout_secs);
  EXPECT_TRUE(config.mounts.validate().ok());
  config.mounts.tape_unload_timeout_secs = 0;
  EXPECT_FALSE(config.mounts.validate().ok());
  EXPECT_NE(std::string::npos, config.mounts.validate().what().find("tape_unload_timeout_secs"));
  config.mounts.tape_unload_timeout_secs = 1;
  EXPECT_TRUE(config.mounts.validate().ok());
}

}  // namespace cta::tape::daemon
