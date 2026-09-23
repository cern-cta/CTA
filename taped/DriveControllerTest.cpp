/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveController.hpp"

#include "common/exception/LostDatabaseConnection.hpp"
#include "common/exception/TimeoutException.hpp"
#include "common/log/StringLogger.hpp"
#include "runtime/config/parsing/TomlParser.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/TapeMount.hpp"
#include "session/ActiveTapeSession.hpp"

#include <functional>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
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
  ActiveTapeSession activeSession;
  TapedConfig config;
  log::StringLogger logger {"host", "DriveControllerTest", log::DEBUG};
  testing::StrictMock<MockDriveScheduler> scheduler;
  std::unique_ptr<DriveController> controller;
  testing::Expectation preparationComplete;
  bool empty = true;
  std::optional<std::string> probeError;
  unsigned int probes = 0;
  unsigned int schedules = 0;
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
      return fixture.activeSession.snapshot();
    }

    IScheduler& scheduler() override { return fixture.scheduler; }

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
    // Iteration tests start from an already prepared drive; registration re-arms cleaning.
    controller->m_cleanBeforeScheduling = false;
  }

  bool liveAt(Clock::time_point now) const { return controller->isLive(now); }

  void waitForLibrary() { controller->waitForLogicalLibrary(); }

  void iteration() { controller->runIteration(); }

  int shutdown() { return controller->shutdownDrive(); }

  bool registerDrive() { return controller->registerDrive(false); }

  void waitForUp() { controller->waitUntilDriveIsRequestedUp(); }

  bool prepare() { return controller->prepareDriveForScheduling(); }

  void requireCleaning() { controller->m_cleanBeforeScheduling = true; }

  void markPrepared() { controller->m_cleanBeforeScheduling = false; }

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
      const auto previousProbes = probes;
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
        EXPECT_EQ(previousProbes, probes);
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
  expectShutdown();
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
    EXPECT_EQ(1, probes);
    return true;
  };
  iteration();
  EXPECT_EQ(1, cleanings);
  EXPECT_EQ(1, schedules);
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
  iteration();
  EXPECT_EQ(1, cleanings);
  EXPECT_EQ(1, probes);
  EXPECT_EQ(0, schedules);
  EXPECT_FALSE(state.up);
  EXPECT_EQ("Operator maintenance", state.reason);
}

// Verify failed cleanup preserves its reason and requires another up request before retrying.
TEST_F(DriveControllerTest, CleaningFailureRequiresAnotherUpRequestAndPreservesReason) {
  requireCleaning();
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
  iteration();
  EXPECT_EQ(1, cleanings);
  EXPECT_EQ(1, probes);
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
    EXPECT_EQ(2, probes);
    return true;
  };
  iteration();
  EXPECT_EQ(2, cleanings);
  EXPECT_FALSE(cleanedVid.has_value());
  EXPECT_EQ(1, schedules);
}

// Verify failed cleanup retains available exception details and keeps the drive down.
TEST_F(DriveControllerTest, CleaningExceptionsKeepDriveDown) {
  for (const int failure : {0, 1, 2, 3}) {
    SCOPED_TRACE(failure);
    requireCleaning();
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
    iteration();
    EXPECT_EQ(failure + 1, probes);
    EXPECT_EQ(0, schedules);
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&scheduler));
  }
}

// Verify one up transition cleans once before scheduling and ordinary retries do not clean again.
TEST_F(DriveControllerTest, UpTransitionCleansOnlyOnceBeforeScheduling) {
  requireCleaning();
  DesiredDriveState up;
  up.up = true;
  testing::InSequence sequence;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Up, _));
  clean = [&] {
    EXPECT_EQ(1, probes);
    EXPECT_EQ(0, schedules);
    return true;
  };
  iteration();
  EXPECT_EQ(1, cleanings);
  EXPECT_FALSE(cleanedVid.has_value());
  expectPreparation();
  iteration();
  EXPECT_EQ(1, cleanings);
  EXPECT_EQ(2, probes);
  EXPECT_EQ(2, schedules);
  EXPECT_THAT(logger.getLog(), testing::Not(testing::HasSubstr("Tape found in drive")));
}

// Unexpected media is diagnostic only; cleanup can still make the drive usable.
TEST_F(DriveControllerTest, TapeBeforePreparationWarnsAndContinuesWithCleanup) {
  testing::InSequence sequence;
  requireCleaning();
  empty = false;
  clean = [&] {
    EXPECT_EQ(1, probes);
    EXPECT_THAT(logger.getLog(), testing::HasSubstr("LVL=\"WARN\""));
    EXPECT_THAT(logger.getLog(),
                testing::HasSubstr("Tape found in drive while preparing to bring it up. Attempting drive cleanup."));
    empty = true;
    return true;
  };
  DesiredDriveState up;
  up.up = true;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Up, _));
  iteration();
  EXPECT_EQ(1, cleanings);
  EXPECT_EQ(1, probes);
  EXPECT_EQ(1, schedules);

  const auto preparationLog = logger.getLog();
  expectPreparation();
  iteration();
  EXPECT_EQ(1, cleanings);
  EXPECT_EQ(2, probes);
  EXPECT_THAT(logger.getLog().substr(preparationLog.size()), testing::Not(testing::HasSubstr("Tape found in drive")));
}

// Neither returned probe errors nor exceptions should block the existing recovery path.
TEST_F(DriveControllerTest, DiagnosticProbeFailuresDoNotPreventCleanup) {
  testing::InSequence sequence;
  for (const int failure : {0, 1, 2}) {
    SCOPED_TRACE(failure);
    requireCleaning();
    empty = false;
    probeError = "Probe failed";
    probe = [failure] {
      if (failure == 1) {
        throw std::runtime_error("Probe failed");
      }
      if (failure == 2) {
        throw 42;
      }
    };
    clean = [&] {
      empty = true;
      probeError.reset();
      probe = {};
      return true;
    };
    DesiredDriveState up;
    up.up = true;
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
    EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _));
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
    EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Up, _));
    iteration();
    EXPECT_EQ(failure + 1, cleanings);
    EXPECT_EQ(failure + 1, schedules);
    EXPECT_EQ(failure + 1, probes);
    EXPECT_THAT(logger.getLog(), testing::Not(testing::HasSubstr("Tape found in drive")));
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&scheduler));
  }
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

// If the desired-state lookup finds no drive entry, the controller registers the missing drive as down.
TEST_F(DriveControllerTest, MissingDriveIsRegisteredDownUntilOperatorUp) {
  DesiredDriveState up;
  up.up = true;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _))
    .WillOnce(Throw(Scheduler::NoSuchDrive("missing")))
    .WillOnce(Throw(Scheduler::NoSuchDrive("missing")))
    .WillOnce(Return(up));
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
  EXPECT_CALL(scheduler, createTapeDriveStatus(_, _, MountType::NoMount, DriveStatus::Down, _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& desired, const auto&, const auto&, const auto&, auto&) {
      EXPECT_FALSE(desired.up);
      EXPECT_EQ(formatDriveDownReason(DriveDownReason::Startup), desired.reason);
    }));
  EXPECT_CALL(scheduler, reportSchedulerBackendName("drive", _));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  waitForUp();
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
  EXPECT_THAT(sleeps, testing::ElementsAre(config.mounts.drive_state_poll_interval_secs));
}

// If a missing drive cannot be registered because of a conflict, desired-state lookup fails.
TEST_F(DriveControllerTest, MissingDriveRegistrationConflictPropagates) {
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Throw(Scheduler::NoSuchDrive("missing")));
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(false));
  EXPECT_THROW(waitForUp(), exception::Exception);
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

  expectShutdown();
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

  expectShutdown();
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

// Controlled exits must not depend on reaching readiness.
TEST_F(DriveControllerTest, UnknownStartupFailuresPublishDownAfterIdentityValidation) {
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
    expectShutdown();
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
  requireCleaning();
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
  iteration();
  EXPECT_EQ(1, probes);
  EXPECT_EQ(1, cleanings);
}

// The catalogue can acknowledge a session's down request before the controller observes it.
TEST_F(DriveControllerTest, ReportedDownWithNewUpRequestRequiresCleaning) {
  previousDrive.emplace();
  previousDrive->driveStatus = DriveStatus::Down;
  previousDrive->desiredUp = true;
  DesiredDriveState up;
  up.up = true;
  testing::InSequence sequence;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Up, _));
  clean = [&] {
    EXPECT_EQ(0, schedules);
    return true;
  };
  iteration();
  EXPECT_EQ(1, stateReads);
  EXPECT_EQ(1, cleanings);
  EXPECT_EQ(1, schedules);
}

TEST_F(DriveControllerTest, FailedPreparationTransitionDoesNotTouchHardware) {
  requireCleaning();
  DesiredDriveState up;
  up.up = true;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _))
    .WillOnce(Throw(std::runtime_error("publication failed")));
  EXPECT_THROW(iteration(), std::runtime_error);
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
TEST_F(DriveControllerTest, DownDriveWaitsForOperatorUpBeforeProbing) {
  testing::InSequence sequence;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  DesiredDriveState up;
  up.up = true;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Up, _));
  iteration();
  EXPECT_EQ(1, probes);
  EXPECT_EQ(1, schedules);
  EXPECT_THAT(sleeps, testing::ElementsAre(config.mounts.drive_state_poll_interval_secs, 7));
}

// If publishing Up fails after a successful probe, the controller must not schedule work.
TEST_F(DriveControllerTest, UpPublicationFailureAfterProbePreventsScheduling) {
  DesiredDriveState up;
  up.up = true;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Up, _))
    .WillOnce(Throw(std::runtime_error("publication failed")));
  EXPECT_THROW(iteration(), std::runtime_error);
  EXPECT_EQ(1, probes);
  EXPECT_EQ(0, schedules);
}

// If the probe finds retained media, the controller requests down for operator inspection.
TEST_F(DriveControllerTest, RetainedTapeRequestsDownWithoutCleaningOrScheduling) {
  empty = false;
  unsigned int cleanAttempts = 0;
  clean = [&] {
    ++cleanAttempts;
    return true;
  };
  expectPreparation();
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& state, auto&) {
      EXPECT_FALSE(state.up);
      EXPECT_EQ(formatDriveDownReason(DriveDownReason::TapeDetected), state.reason);
    }));

  iteration();
  EXPECT_EQ(0, cleanAttempts);
  EXPECT_EQ(1, probes);
  EXPECT_EQ(0, schedules);
}

// If the drive probe fails, the controller publishes the probe failure as the down reason.
TEST_F(DriveControllerTest, ProbeFailurePublishesItsReasonAndPreventsScheduling) {
  empty = false;
  probeError = "Cannot open drive";
  expectPreparation();
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([this](const auto&, const DesiredDriveState& state, auto&) {
      EXPECT_FALSE(state.up);
      EXPECT_EQ(formatDriveDownReason(DriveDownReason::DriveProbeFailed, *probeError), state.reason);
    }));
  iteration();
  EXPECT_EQ(0, schedules);
}

// If scheduling returns no mount, the controller waits before the next attempt.
TEST_F(DriveControllerTest, IdleMountWaitsAndRechecksDriveBeforeRetry) {
  expectPreparation();
  iteration();
  expectPreparation();
  iteration();
  EXPECT_EQ(2, probes);
  EXPECT_EQ(2, schedules);
  EXPECT_EQ(0, transfers);
  EXPECT_THAT(
    sleeps,
    testing::ElementsAre(config.mounts.idle_scheduling_interval_secs, config.mounts.idle_scheduling_interval_secs));
}

// Scheduling resumes with cleaning and a fresh probe after an operator down/up request.
TEST_F(DriveControllerTest, OperatorDownUpRequiresCleaningAndAnotherProbe) {
  expectPreparation();
  iteration();

  {
    testing::InSequence sequence;
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
    EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
    DesiredDriveState up;
    up.up = true;
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
    EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _));
    expectPreparation();
  }
  iteration();

  EXPECT_EQ(1, cleanings);
  EXPECT_EQ(2, probes);
  EXPECT_EQ(2, schedules);
  EXPECT_THAT(sleeps,
              testing::ElementsAre(config.mounts.idle_scheduling_interval_secs,
                                   config.mounts.drive_state_poll_interval_secs,
                                   config.mounts.idle_scheduling_interval_secs));
}

// If mount scheduling times out, the controller waits and allows a later iteration.
TEST_F(DriveControllerTest, SchedulingTimeoutWaitsAndAllowsAnotherIteration) {
  schedule = []() -> std::unique_ptr<TapeMount> { throw exception::TimeoutException("timeout"); };
  expectPreparation();
  EXPECT_NO_THROW(iteration());
  schedule = {};
  expectPreparation();
  iteration();
  EXPECT_EQ(2, probes);
  EXPECT_EQ(2, schedules);
  EXPECT_THAT(sleeps, testing::ElementsAre(7, 7));
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
  EXPECT_EQ(2, probes);
  EXPECT_EQ(2, schedules);
  EXPECT_THAT(
    sleeps,
    testing::ElementsAre(config.mounts.idle_scheduling_interval_secs, config.mounts.idle_scheduling_interval_secs));
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
  EXPECT_THAT(sleeps, testing::ElementsAre(config.mounts.idle_scheduling_interval_secs));

  supplyMount();
  expectPreparation();
  EXPECT_NO_THROW(iteration());
  EXPECT_EQ(2, probes);
  EXPECT_EQ(2, schedules);
  EXPECT_EQ(1, transfers);
  EXPECT_EQ(1, destroyed);
  EXPECT_EQ(nullptr, liveMount());
  EXPECT_EQ(0, cleanAttempts);
  EXPECT_THAT(sleeps, testing::ElementsAre(config.mounts.idle_scheduling_interval_secs));
}

// Scheduling without work does not start a TapeSession.
TEST_F(DriveControllerTest, IdleDriveDoesNotStartATapeSession) {
  expectPreparation();

  iteration();

  EXPECT_EQ(0, transfers);
  EXPECT_EQ(nullptr, liveMount());
}

// Transfers and recovery.

// A reusable drive without a recovery request remains available for scheduling.
TEST_F(DriveControllerTest, ReusableDriveWithoutRecoveryDoesNotRequestDown) {
  supplyMount();
  transfer = [](TapeMount&) {
    TapeSessionResult result;
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
  EXPECT_EQ(2, probes);
  EXPECT_EQ(2, destroyed);
  EXPECT_EQ(nullptr, liveMount());
}

// A handled finalization failure has already completed the session's local cleanup.
TEST_F(DriveControllerTest, HandledSessionFailureRetriesWithoutCleaning) {
  supplyMount();
  transfer = [](TapeMount&) {
    TapeSessionResult result;
    result.retryDelayRequired = true;
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
  EXPECT_THAT(sleeps, testing::ElementsAre(config.mounts.idle_scheduling_interval_secs));
}

// Escaping session exceptions are fatal, including database disconnections.
TEST_F(DriveControllerTest, EscapingSessionExceptionsExitWithoutRetrying) {
  for (const auto failure : {"standard", "database", "unknown"}) {
    SCOPED_TRACE(failure);
    testing::InSequence sequence;
    expectRunStartup();
    DesiredDriveState up;
    up.up = true;
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
    EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _));
    expectPreparation();
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
    const auto previousTransfers = transfers;
    const auto previousSchedules = schedules;
    const auto previousDestroyed = destroyed;
    unsigned int cleanAttempts = 0;
    clean = [&] {
      ++cleanAttempts;
      EXPECT_EQ(nullptr, liveMount());
      EXPECT_EQ(previousDestroyed, destroyed);
      return true;
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
    EXPECT_EQ(1, cleanAttempts);
    EXPECT_EQ(previousTransfers + 1, transfers);
    EXPECT_EQ(previousSchedules + 1, schedules);
    EXPECT_TRUE(sleeps.empty());
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&scheduler));
  }
}

// If a transfer reports an unusable drive with a specific reason, the controller requests it down.
TEST_F(DriveControllerTest, UnusableDriveRequestsDownAndPreservesSpecificReason) {
  supplyMount();
  transfer = [](TapeMount&) {
    TapeSessionResult result;
    result.driveUsability = DriveUsability::MustRemainDown;
    return result;
  };
  expectPreparation();
  DesiredDriveState state;
  state.reason = "Unload failed";
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).After(preparationComplete).WillOnce(Return(state));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& desired, auto&) {
      EXPECT_FALSE(desired.up);
      EXPECT_FALSE(desired.reason);
    }));
  iteration();
  EXPECT_EQ(1, destroyed);

  // Recover using the session VID even after its mount has been destroyed.
  schedule = {};
  DesiredDriveState up;
  up.up = true;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).Times(2).WillRepeatedly(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Up, _));
  iteration();
  EXPECT_EQ("V00001", cleanedVid);

  // Successful ejection must not carry that VID into an unrelated cleanup.
  requireCleaning();
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).Times(2).WillRepeatedly(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Up, _));
  iteration();
  EXPECT_FALSE(cleanedVid.has_value());
}

// If a transfer marks the drive unusable without a specific reason, the controller requests it down.
TEST_F(DriveControllerTest, UnusableDriveWithoutSpecificReasonPublishesTransferFailure) {
  supplyMount();
  transfer = [](TapeMount&) {
    TapeSessionResult result;
    result.driveUsability = DriveUsability::MustRemainDown;
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
    result.driveUsability = DriveUsability::MustRemainDown;
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
    result.driveUsability = DriveUsability::MustRemainDown;
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
  EXPECT_EQ(1, probes);
  EXPECT_EQ(1, transfers);
  EXPECT_EQ(0, cleanings);

  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(failed));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Throw(std::runtime_error("end wait")));
  EXPECT_THROW(iteration(), std::runtime_error);
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(failed));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& desired, auto&) {
      EXPECT_FALSE(desired.up);
      EXPECT_FALSE(desired.reason);
    }));
  EXPECT_EQ(0, shutdown());
  EXPECT_EQ(1, probes);
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

// An unknown transfer exception unwinds the active mount before publishing down.
TEST_F(DriveControllerTest, UnknownIterationExceptionReleasesMountWithoutFinalCleaning) {
  testing::InSequence sequence;
  expectRunStartup();
  DesiredDriveState up;
  up.up = true;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::CleaningUp, _));
  expectPreparation();
  supplyMount();
  transfer = [](TapeMount&) -> TapeSessionResult { throw 42; };
  unsigned int cleanAttempts = 0;
  clean = [&] {
    ++cleanAttempts;
    EXPECT_EQ(nullptr, liveMount());
    EXPECT_EQ(0, destroyed);
    return true;
  };
  DesiredDriveState state;
  state.reason = "Operator intervention";
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(state));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([&](const auto&, const DesiredDriveState& desired, auto&) {
      EXPECT_EQ(1, cleanAttempts);
      EXPECT_FALSE(desired.up);
      EXPECT_FALSE(desired.reason);
    }));

  EXPECT_EQ(1, controller->run());
  EXPECT_FALSE(controller->isReady());
  EXPECT_EQ(1, cleanAttempts);
  EXPECT_EQ(1, transfers);
  EXPECT_EQ(1, destroyed);
  EXPECT_EQ(nullptr, liveMount());
  EXPECT_THAT(logger.getLog(), testing::HasSubstr("Drive controller failed with an unknown exception"));
}

// Shutdown publishes down without invoking the cleaner.
TEST_F(DriveControllerTest, ShutdownPublishesDownWithoutCleaning) {
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
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _))
    .WillOnce(Throw(std::runtime_error("reported state failed")));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _));
  EXPECT_EQ(1, shutdown());
  EXPECT_EQ(0, cleanings);
}

// A desired-state publication failure makes shutdown return failure.
TEST_F(DriveControllerTest, ShutdownPublicationFailureReturnsNonzero) {
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
  // Startup normally arms cleaning; isolate stopping after an idle scheduling iteration.
  libraryExists = [&] {
    markPrepared();
    return true;
  };
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
  libraryExists = [&] {
    markPrepared();
    return true;
  };
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
  const ActiveTapeSession::Scope active(activeSession, tracker);
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
  const ActiveTapeSession::Scope active(activeSession, tracker);
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
      const ActiveTapeSession::Scope active(activeSession, tracker);
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
    EXPECT_FALSE(activeSession.snapshot());
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
