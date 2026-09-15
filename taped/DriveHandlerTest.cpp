/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveHandler.hpp"

#include "common/exception/LostDatabaseConnection.hpp"
#include "common/exception/TimeoutException.hpp"
#include "common/log/DummyLogger.hpp"

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

class DriveHandlerTest : public testing::Test {
protected:
  TapedConfig config;
  log::DummyLogger logger {"host", "DriveHandlerTest"};
  testing::StrictMock<MockDriveScheduler> scheduler;
  std::unique_ptr<DriveHandler> handler;
  testing::Expectation preparationComplete;
  bool empty = true;
  std::optional<std::string> probeError;
  unsigned int probes = 0;
  unsigned int schedules = 0;
  unsigned int transfers = 0;
  unsigned int destroyed = 0;
  std::vector<unsigned int> sleeps;
  std::function<std::unique_ptr<TapeMount>()> schedule;
  std::function<TransferSessionResult(TapeMount&)> transfer;
  std::function<bool()> libraryExists = [] { return true; };
  std::function<bool()> clean = [] { return true; };

  void SetUp() override {
    config.drive.name = "drive";
    config.mounts.idle_scheduling_interval_secs = 7;
    DriveHandler::Operations operations;
    operations.logicalLibraryExists = [this] { return libraryExists(); };
    operations.probeDrive = [this] {
      ++probes;
      EXPECT_EQ(nullptr, mount());
      return std::make_pair(empty, probeError);
    };
    operations.getNextMount = [this] {
      ++schedules;
      return schedule ? schedule() : nullptr;
    };
    operations.transfer = [this](TapeMount& tapeMount) {
      ++transfers;
      EXPECT_EQ(&tapeMount, mount());
      return transfer ? transfer(tapeMount) : TransferSessionResult {};
    };
    operations.clean = [this] { return clean(); };
    operations.sleep = [this](unsigned int seconds) {
      if (sleeps.size() >= 10) {
        throw std::runtime_error("Unexpected repeated handler wait");
      }
      sleeps.push_back(seconds);
    };
    handler = std::make_unique<DriveHandler>(config, logger, scheduler, std::move(operations));
  }

  void waitForLibrary() { handler->waitForLogicalLibrary(); }

  void iteration() { handler->runIteration(); }

  int shutdown() { return handler->shutdownDrive(); }

  bool registerDrive() { return handler->registerDrive(false); }

  DesiredDriveState desiredState() { return handler->getDesiredDriveState(); }

  TapeMount* mount() { return handler->m_tapeSessionTracker.mount(); }

  void down(bool preserve = false) { handler->putDriveDown(DriveDownReason::Shutdown, {}, preserve); }

  void expectPreparation() {
    testing::InSequence sequence;
    DesiredDriveState state;
    state.up = true;
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(state));
    EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Probing, _));
    if (empty) {
      preparationComplete = EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Up, _));
    }
  }

  void supplyMount() {
    schedule = [this] {
      auto tapeMount = std::make_unique<TestMount>();
      tapeMount->onDestroy = [this] {
        EXPECT_EQ(nullptr, mount());
        ++destroyed;
      };
      return tapeMount;
    };
  }
};

TEST_F(DriveHandlerTest, RegistrationConflictStopsStartup) {
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(false));
  EXPECT_EQ(1, handler->run());
  EXPECT_EQ(0, schedules);
}

TEST_F(DriveHandlerTest, RegistrationPreservesOperatorReasonAndComment) {
  DesiredDriveState state;
  state.up = true;
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

TEST_F(DriveHandlerTest, MissingDriveIsRegisteredDown) {
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _))
    .Times(2)
    .WillRepeatedly(Throw(Scheduler::NoSuchDrive("missing")));
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
  EXPECT_CALL(scheduler, createTapeDriveStatus(_, _, MountType::NoMount, DriveStatus::Down, _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& desired, const auto&, const auto&, const auto&, auto&) {
      EXPECT_FALSE(desired.up);
      EXPECT_EQ(formatDriveDownReason(DriveDownReason::Startup), desired.reason);
    }));
  EXPECT_CALL(scheduler, reportSchedulerBackendName("drive", _));
  EXPECT_FALSE(desiredState().up);
}

TEST_F(DriveHandlerTest, DesiredStateDatabaseFailurePropagates) {
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _))
    .WillOnce(Throw(exception::LostDatabaseConnection("database unavailable")));
  EXPECT_THROW(iteration(), exception::LostDatabaseConnection);
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
}

TEST_F(DriveHandlerTest, IdleMountWaitsAndRechecksDriveBeforeRetry) {
  expectPreparation();
  iteration();
  expectPreparation();
  iteration();
  EXPECT_EQ(2, probes);
  EXPECT_EQ(2, schedules);
  EXPECT_EQ(0, transfers);
  EXPECT_THAT(sleeps, testing::ElementsAre(7, 7));
}

TEST_F(DriveHandlerTest, SchedulingTimeoutWaitsAndAllowsAnotherIteration) {
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

TEST_F(DriveHandlerTest, SchedulingDatabaseFailurePropagatesWithoutRetryWait) {
  schedule = []() -> std::unique_ptr<TapeMount> { throw exception::LostDatabaseConnection("database unavailable"); };
  expectPreparation();
  EXPECT_THROW(iteration(), exception::LostDatabaseConnection);
  EXPECT_TRUE(sleeps.empty());
  EXPECT_EQ(nullptr, mount());
}

TEST_F(DriveHandlerTest, RetainedTapePreventsScheduling) {
  empty = false;
  expectPreparation();
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& state, auto&) {
      EXPECT_FALSE(state.up);
      EXPECT_EQ(formatDriveDownReason(DriveDownReason::TapeDetected), state.reason);
    }));
  iteration();
  EXPECT_EQ(0, schedules);
}

TEST_F(DriveHandlerTest, ProbeFailurePublishesItsReasonAndPreventsScheduling) {
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

TEST_F(DriveHandlerTest, FileFailureWithReusableDriveDoesNotRequestDown) {
  supplyMount();
  transfer = [](TapeMount&) {
    TransferSessionResult result;
    result.transferOutcome = TransferSessionResult::Outcome::Failure;
    return result;
  };
  expectPreparation();
  iteration();
  EXPECT_EQ(1, transfers);
  EXPECT_EQ(1, destroyed);
  EXPECT_TRUE(sleeps.empty());
}

TEST_F(DriveHandlerTest, TransferExceptionReleasesMountAndProbesBeforeNextSession) {
  supplyMount();
  transfer = [](TapeMount&) -> TransferSessionResult { throw std::runtime_error("transfer failed"); };
  expectPreparation();
  EXPECT_NO_THROW(iteration());
  EXPECT_EQ(1, destroyed);
  transfer = {};
  expectPreparation();
  iteration();
  EXPECT_EQ(2, destroyed);
  EXPECT_EQ(2, probes);
  EXPECT_EQ(2, transfers);
  EXPECT_THAT(sleeps, testing::ElementsAre(7));
}

TEST_F(DriveHandlerTest, TransferDatabaseFailureReleasesMountAndPropagates) {
  supplyMount();
  transfer = [](TapeMount&) -> TransferSessionResult {
    throw exception::LostDatabaseConnection("database unavailable");
  };
  expectPreparation();
  EXPECT_THROW(iteration(), exception::LostDatabaseConnection);
  EXPECT_EQ(1, destroyed);
  EXPECT_TRUE(sleeps.empty());
}

TEST_F(DriveHandlerTest, UnusableDriveRequestsDownAndPreservesSpecificReason) {
  supplyMount();
  transfer = [](TapeMount&) {
    TransferSessionResult result;
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
}

TEST_F(DriveHandlerTest, DownPublicationsBothRunAndFirstExceptionIsPreserved) {
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

TEST_F(DriveHandlerTest, DesiredPublicationFailurePropagates) {
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _)).WillOnce(Throw(std::runtime_error("failed")));
  EXPECT_THROW(down(), std::runtime_error);
}

TEST_F(DriveHandlerTest, ReasonLookupFailureStillAttemptsBothDownPublications) {
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Throw(std::runtime_error("lookup failed")));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& state, auto&) {
      EXPECT_FALSE(state.up);
      EXPECT_FALSE(state.reason);
    }));
  EXPECT_THROW(down(true), std::runtime_error);
}

TEST_F(DriveHandlerTest, SuccessfulShutdownPublishesCleanReason) {
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& state, auto&) {
      EXPECT_EQ(formatDriveDownReason(DriveDownReason::Shutdown), state.reason);
    }));
  EXPECT_EQ(0, shutdown());
}

TEST_F(DriveHandlerTest, FailedShutdownCleaningStillPublishesDown) {
  clean = [] { return false; };
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& state, auto&) {
      EXPECT_EQ(formatDriveDownReason(DriveDownReason::CleanerFailed), state.reason);
    }));
  EXPECT_EQ(1, shutdown());
}

TEST_F(DriveHandlerTest, ShutdownCleaningExceptionStillAttemptsDownAfterPublicationFailure) {
  clean = []() -> bool { throw std::runtime_error("cleaner failed"); };
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _))
    .WillOnce(Throw(std::runtime_error("reported state failed")));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _));
  EXPECT_EQ(1, shutdown());
}

TEST_F(DriveHandlerTest, MissingLogicalLibraryWaitsUntilAvailable) {
  unsigned int checks = 0;
  libraryExists = [&] { return ++checks == 3; };
  waitForLibrary();
  EXPECT_EQ(3, checks);
  EXPECT_THAT(
    sleeps,
    testing::ElementsAre(config.mounts.drive_state_poll_interval_secs, config.mounts.drive_state_poll_interval_secs));
  EXPECT_EQ(0, schedules);
}

TEST_F(DriveHandlerTest, LogicalLibraryDatabaseFailurePropagatesWithoutWaiting) {
  libraryExists = []() -> bool { throw exception::LostDatabaseConnection("database unavailable"); };
  EXPECT_THROW(waitForLibrary(), exception::LostDatabaseConnection);
  EXPECT_TRUE(sleeps.empty());
}

TEST_F(DriveHandlerTest, DownDriveWaitsForOperatorUpBeforeProbing) {
  testing::InSequence sequence;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  DesiredDriveState up;
  up.up = true;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Probing, _));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Up, _));
  iteration();
  EXPECT_EQ(1, probes);
  EXPECT_EQ(1, schedules);
  EXPECT_THAT(sleeps, testing::ElementsAre(config.mounts.drive_state_poll_interval_secs, 7));
}

TEST_F(DriveHandlerTest, ProbingPublicationFailurePreventsHardwareAccessAndScheduling) {
  DesiredDriveState up;
  up.up = true;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(up));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Probing, _))
    .WillOnce(Throw(std::runtime_error("publication failed")));
  EXPECT_THROW(iteration(), std::runtime_error);
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
}

TEST_F(DriveHandlerTest, MissingDriveRegistrationConflictPropagates) {
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Throw(Scheduler::NoSuchDrive("missing")));
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(false));
  EXPECT_THROW(desiredState(), exception::Exception);
}

TEST_F(DriveHandlerTest, UnusableDriveWithoutSpecificReasonPublishesTransferFailure) {
  supplyMount();
  transfer = [](TapeMount&) {
    TransferSessionResult result;
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
      EXPECT_EQ(formatDriveDownReason(DriveDownReason::TransferSessionFailed), state.reason);
    }));
  iteration();
  EXPECT_EQ(1, destroyed);
}

TEST_F(DriveHandlerTest, DownPublicationFailureAfterTransferStillReleasesMount) {
  supplyMount();
  transfer = [](TapeMount&) {
    TransferSessionResult result;
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

TEST_F(DriveHandlerTest, SuccessfulCleaningWithPublicationFailureReturnsNonzero) {
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _)).WillOnce(Throw(std::runtime_error("publication failed")));
  EXPECT_EQ(1, shutdown());
}

TEST_F(DriveHandlerTest, TransferExceptionFollowedByRetainedMediaPreventsAnotherMount) {
  supplyMount();
  transfer = [](TapeMount&) -> TransferSessionResult { throw std::runtime_error("transfer failed"); };
  expectPreparation();
  iteration();
  empty = false;
  expectPreparation();
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _));
  iteration();
  EXPECT_EQ(2, probes);
  EXPECT_EQ(1, schedules);
  EXPECT_EQ(1, transfers);
  EXPECT_EQ(1, destroyed);
}

TEST_F(DriveHandlerTest, RegistrationReplacesAbsentAndCleanShutdownReasonsWithStartup) {
  for (const auto& reason :
       std::vector<std::optional<std::string>> {std::nullopt, formatDriveDownReason(DriveDownReason::Shutdown)}) {
    SCOPED_TRACE(reason.value_or("absent"));
    DesiredDriveState state;
    state.reason = reason;
    EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(state));
    EXPECT_CALL(scheduler, createTapeDriveStatus(_, _, MountType::NoMount, DriveStatus::Down, _, _))
      .WillOnce(Invoke([](const auto&, const DesiredDriveState& desired, const auto&, const auto&, const auto&, auto&) {
        EXPECT_FALSE(desired.up);
        EXPECT_EQ(formatDriveDownReason(DriveDownReason::Startup), desired.reason);
      }));
    EXPECT_CALL(scheduler, reportSchedulerBackendName("drive", _));
    EXPECT_TRUE(registerDrive());
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&scheduler));
  }
}

TEST_F(DriveHandlerTest, ShutdownReplacesStartupAndCleanReasonsButPreservesOperatorReason) {
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

}  // namespace cta::tape::daemon
