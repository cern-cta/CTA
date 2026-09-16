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

/*
 * If the drive name is owned by another host or logical library, registration fails.
 * Startup must stop before probing or requesting work for that drive.
 */
TEST_F(DriveHandlerTest, RegistrationConflictStopsStartup) {
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(false));
  EXPECT_EQ(1, handler->run());
  EXPECT_EQ(0, schedules);
}

/*
 * If the drive already has an operator reason and comment, registration preserves both.
 * Startup must not erase an operator's explanation or make the drive available.
 */
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

/*
 * If the desired-state lookup finds no drive entry, the handler registers the missing drive as down.
 * It must wait for an explicit up request before using the drive.
 */
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

/*
 * If the desired-state lookup loses its database connection, the iteration propagates the error.
 * Probing and scheduling cannot proceed without a reliable operator state.
 */
TEST_F(DriveHandlerTest, DesiredStateDatabaseFailurePropagates) {
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _))
    .WillOnce(Throw(exception::LostDatabaseConnection("database unavailable")));
  EXPECT_THROW(iteration(), exception::LostDatabaseConnection);
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
}

/*
 * If scheduling returns no mount, the handler waits before the next attempt.
 * It checks the drive state and probes again before asking for more work.
 */
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

/*
 * If mount scheduling times out, the handler waits and allows a later iteration.
 * A timeout should not permanently stop an otherwise usable drive.
 */
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

// TODO: if scheduling loses database connection, it should log an error, wait for the scheduler to become available again and then continue normally
/*
 * If scheduling loses its database connection, the error propagates without an idle retry wait.
 * The handler cannot treat a backend outage as an empty queue.
 */
TEST_F(DriveHandlerTest, SchedulingDatabaseFailurePropagatesWithoutRetryWait) {
  schedule = []() -> std::unique_ptr<TapeMount> { throw exception::LostDatabaseConnection("database unavailable"); };
  expectPreparation();
  EXPECT_THROW(iteration(), exception::LostDatabaseConnection);
  EXPECT_TRUE(sleeps.empty());
  EXPECT_EQ(nullptr, mount());
}

// TODO: if a transfer session fails to clean, we may still want to try the CleanerSession?
/*
 * If the probe finds a tape still in the drive, the handler requests the drive down.
 * It must not schedule another mount onto occupied hardware.
 */
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

/*
 * If the drive probe fails, the handler publishes the probe failure as the down reason.
 * Scheduling must stop because the drive's empty state is unknown.
 */
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

/*
 * If a transfer reports a file failure but the drive remains reusable, the handler leaves it available.
 * A failed file alone does not establish a hardware problem.
 */
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

/*
 * If a transfer throws, the handler releases its mount before a later scheduling attempt.
 * The next iteration probes the drive again in case the tape was retained.
 */
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

// TODO: this is not correct right? if a transfer throws an exception, we should try to clean the drive. If that succeeds fine, we can just continue with the next iteration
/*
 * If a transfer throws an ordinary exception, the handler should clean the drive and request it down.
 * The exception leaves the session's hardware state uncertain.
 */
TEST_F(DriveHandlerTest, OrdinaryTransferExceptionCleansDriveAndRequestsDown) {
  supplyMount();
  transfer = [](TapeMount&) -> TransferSessionResult { throw std::runtime_error("transfer failed"); };
  unsigned int cleanAttempts = 0;
  clean = [&] {
    ++cleanAttempts;
    return true;
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

  EXPECT_NO_THROW(iteration());
  EXPECT_EQ(1, cleanAttempts);
  EXPECT_EQ(1, destroyed);
  EXPECT_EQ(nullptr, mount());
}

/*
 * If transfer recovery cleaning returns failure, the handler should request the drive down.
 * The cleaner failure must be reported instead of allowing another mount.
 */
TEST_F(DriveHandlerTest, TransferExceptionWithFailedCleanupRequestsCleanerFailureDown) {
  supplyMount();
  transfer = [](TapeMount&) -> TransferSessionResult { throw std::runtime_error("transfer failed"); };
  unsigned int cleanAttempts = 0;
  clean = [&] {
    ++cleanAttempts;
    return false;
  };
  expectPreparation();
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _))
    .After(preparationComplete)
    .WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& state, auto&) {
      EXPECT_FALSE(state.up);
      EXPECT_EQ(formatDriveDownReason(DriveDownReason::CleanerFailed), state.reason);
    }));

  EXPECT_NO_THROW(iteration());
  EXPECT_EQ(1, cleanAttempts);
  EXPECT_EQ(1, destroyed);
  EXPECT_EQ(nullptr, mount());
}

/*
 * If transfer recovery cleaning throws, the handler should still request the drive down.
 * A cleaner exception must not leave the drive available for scheduling.
 */
TEST_F(DriveHandlerTest, TransferExceptionWithThrowingCleanupRequestsCleanerFailureDown) {
  supplyMount();
  transfer = [](TapeMount&) -> TransferSessionResult { throw std::runtime_error("transfer failed"); };
  unsigned int cleanAttempts = 0;
  clean = [&]() -> bool {
    ++cleanAttempts;
    throw std::runtime_error("cleaner failed");
  };
  expectPreparation();
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _))
    .After(preparationComplete)
    .WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& state, auto&) {
      EXPECT_FALSE(state.up);
      EXPECT_EQ(formatDriveDownReason(DriveDownReason::CleanerFailed), state.reason);
    }));

  EXPECT_NO_THROW(iteration());
  EXPECT_EQ(1, cleanAttempts);
  EXPECT_EQ(1, destroyed);
  EXPECT_EQ(nullptr, mount());
}

// TODO: if this is the case, it should clean the drive and then wait for the DB to be up again before continuing.
// In general: lost DB connection errors are transient so we want the drivehandler to be able to recover without operator intervention
/*
 * If a transfer loses its database connection, the handler releases the mount and propagates the error.
 * It must not silently retry scheduling while the backend is unavailable.
 */
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

// TODO: not correct: lost connection is transient so no reason to put the drive down (unless cleaning fails)
/*
 * If a transfer loses its object-store connection, the handler should clean and mark the drive down.
 * Hardware recovery must be attempted before the database error escapes.
 */
TEST_F(DriveHandlerTest, TransferDatabaseFailureCleansDriveAndPublishesDownBeforePropagating) {
  supplyMount();
  transfer = [](TapeMount&) -> TransferSessionResult {
    throw exception::LostDatabaseConnection("objectstore unavailable");
  };
  unsigned int cleanAttempts = 0;
  clean = [&] {
    ++cleanAttempts;
    return true;
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

  EXPECT_THROW(iteration(), exception::LostDatabaseConnection);
  EXPECT_EQ(1, cleanAttempts);
  EXPECT_EQ(1, destroyed);
  EXPECT_EQ(nullptr, mount());
  EXPECT_TRUE(sleeps.empty());
}

/*
 * If post-transfer cleaning reports failure, the handler should request the drive down.
 * A subsequent mount is unsafe until the drive is recovered.
 */
TEST_F(DriveHandlerTest, FailedPostTransferCleaningRequestsDriveDown) {
  supplyMount();
  unsigned int cleanAttempts = 0;
  clean = [&] {
    ++cleanAttempts;
    return false;
  };
  expectPreparation();
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _))
    .After(preparationComplete)
    .WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& state, auto&) {
      EXPECT_FALSE(state.up);
      EXPECT_EQ(formatDriveDownReason(DriveDownReason::CleanerFailed), state.reason);
    }));

  iteration();
  EXPECT_EQ(1, cleanAttempts);
  EXPECT_EQ(1, destroyed);
  EXPECT_EQ(nullptr, mount());
}

/*
 * If post-transfer cleaning throws, the handler should request the drive down.
 * An exception cannot be treated as evidence that cleanup succeeded.
 */
TEST_F(DriveHandlerTest, PostTransferCleanerExceptionRequestsDriveDown) {
  supplyMount();
  unsigned int cleanAttempts = 0;
  clean = [&]() -> bool {
    ++cleanAttempts;
    throw std::runtime_error("cleaner failed");
  };
  expectPreparation();
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _))
    .After(preparationComplete)
    .WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& state, auto&) {
      EXPECT_FALSE(state.up);
      EXPECT_EQ(formatDriveDownReason(DriveDownReason::CleanerFailed), state.reason);
    }));

  EXPECT_NO_THROW(iteration());
  EXPECT_EQ(1, cleanAttempts);
  EXPECT_EQ(1, destroyed);
  EXPECT_EQ(nullptr, mount());
}

/*
 * If a transfer reports an unusable drive with a specific reason, the handler requests it down.
 * It preserves that reason so the more precise diagnosis is not overwritten.
 */
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

/*
 * If publishing the reported down status fails, the handler still tries the desired down state.
 * When both publications fail, the first error remains the one propagated.
 */
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

/*
 * If publishing the desired down state fails, the handler propagates the error.
 * The caller must know that the drive was not reliably taken out of service.
 */
TEST_F(DriveHandlerTest, DesiredPublicationFailurePropagates) {
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _)).WillOnce(Throw(std::runtime_error("failed")));
  EXPECT_THROW(down(), std::runtime_error);
}

/*
 * If reading the existing down reason fails, the handler still attempts both down publications.
 * It avoids overwriting an unknown reason while preserving the lookup error.
 */
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

/*
 * If final cleaning succeeds, shutdown publishes the clean-shutdown reason and returns success.
 * That reason distinguishes an orderly stop from a drive failure.
 */
TEST_F(DriveHandlerTest, SuccessfulShutdownPublishesCleanReason) {
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& state, auto&) {
      EXPECT_EQ(formatDriveDownReason(DriveDownReason::Shutdown), state.reason);
    }));
  EXPECT_EQ(0, shutdown());
}

/*
 * If final cleaning returns failure, shutdown still publishes the drive as down.
 * It records the cleaner failure and returns a nonzero result.
 */
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

/*
 * If final cleaning throws, shutdown still attempts both down-state publications.
 * A publication failure must not prevent the other publication attempt.
 */
TEST_F(DriveHandlerTest, ShutdownCleaningExceptionStillAttemptsDownAfterPublicationFailure) {
  clean = []() -> bool { throw std::runtime_error("cleaner failed"); };
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _))
    .WillOnce(Throw(std::runtime_error("reported state failed")));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _));
  EXPECT_EQ(1, shutdown());
}

/*
 * If the configured logical library is absent, startup waits for it to appear.
 * It must not request mounts before the library exists.
 */
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

/*
 * If the logical-library lookup loses its database connection, startup propagates the error.
 * A backend failure must not be mistaken for a library awaiting creation.
 */
TEST_F(DriveHandlerTest, LogicalLibraryDatabaseFailurePropagatesWithoutWaiting) {
  libraryExists = []() -> bool { throw exception::LostDatabaseConnection("database unavailable"); };
  EXPECT_THROW(waitForLibrary(), exception::LostDatabaseConnection);
  EXPECT_TRUE(sleeps.empty());
}

/*
 * If publishing the drive registration fails, run should end with a failure result.
 * It must not check the library or schedule work for an unregistered drive.
 */
TEST_F(DriveHandlerTest, RegistrationPublicationFailureAbortsStartupBeforeLibraryOrScheduling) {
  unsigned int libraryChecks = 0;
  libraryExists = [&] {
    ++libraryChecks;
    return true;
  };
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, createTapeDriveStatus(_, _, MountType::NoMount, DriveStatus::Down, _, _))
    .WillOnce(Throw(std::runtime_error("registration publication failed")));

  EXPECT_EQ(1, handler->run());
  EXPECT_EQ(0, libraryChecks);
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
}

/*
 * If publishing the scheduler backend name fails, run should end with a failure result.
 * The drive must not begin scheduling with incomplete registration.
 */
TEST_F(DriveHandlerTest, SchedulerBackendPublicationFailureAbortsStartupBeforeLibraryOrScheduling) {
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

  EXPECT_EQ(1, handler->run());
  EXPECT_EQ(0, libraryChecks);
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
}

/*
 * If registration loses its database connection, run should end with a failure result.
 * Library checks and scheduling must not start without a registered drive.
 */
TEST_F(DriveHandlerTest, StartupDatabaseFailureAbortsBeforeLibraryOrScheduling) {
  unsigned int libraryChecks = 0;
  libraryExists = [&] {
    ++libraryChecks;
    return true;
  };
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _))
    .WillOnce(Throw(exception::LostDatabaseConnection("objectstore unavailable")));

  EXPECT_EQ(1, handler->run());
  EXPECT_EQ(0, libraryChecks);
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
}

/*
 * If the logical-library lookup loses its database connection, run should end with a failure result.
 * Scheduling cannot begin until startup has verified the library.
 */
TEST_F(DriveHandlerTest, LogicalLibraryDatabaseFailureEndsStartupWithoutScheduling) {
  libraryExists = []() -> bool { throw exception::LostDatabaseConnection("objectstore unavailable"); };
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(true));
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, createTapeDriveStatus(_, _, MountType::NoMount, DriveStatus::Down, _, _));
  EXPECT_CALL(scheduler, reportSchedulerBackendName("drive", _));

  EXPECT_EQ(1, handler->run());
  EXPECT_EQ(0, probes);
  EXPECT_EQ(0, schedules);
}

/*
 * If the desired state remains down, the handler refreshes its reported status and waits.
 * It probes and schedules only after the operator requests the drive up.
 */
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

/*
 * If publishing the probing status fails, the handler stops before touching hardware.
 * The backend must reflect the transition before the drive is probed.
 */
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

/*
 * If a missing drive cannot be registered because of a conflict, desired-state lookup fails.
 * The handler must not proceed under an entry owned elsewhere.
 */
TEST_F(DriveHandlerTest, MissingDriveRegistrationConflictPropagates) {
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Throw(Scheduler::NoSuchDrive("missing")));
  EXPECT_CALL(scheduler, checkDriveCanBeCreated(_, _)).WillOnce(Return(false));
  EXPECT_THROW(desiredState(), exception::Exception);
}

/*
 * If a transfer marks the drive unusable without a specific reason, the handler requests it down.
 * It supplies a transfer-failure reason so the down state is explained.
 */
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

/*
 * If down-state publication fails after an unusable transfer, the error propagates.
 * The handler must still release the mount and clear the tracker reference.
 */
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

/*
 * If final cleaning succeeds but down-state publication fails, shutdown returns failure.
 * Successful hardware cleanup cannot hide an unreported drive state.
 */
TEST_F(DriveHandlerTest, SuccessfulCleaningWithPublicationFailureReturnsNonzero) {
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(DesiredDriveState {}));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _)).WillOnce(Throw(std::runtime_error("publication failed")));
  EXPECT_EQ(1, shutdown());
}

/*
 * If a transfer throws and the next probe finds retained media, the handler requests down.
 * It must not schedule another mount while the previous tape remains in the drive.
 */
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

/*
 * If the drive has no down reason or was cleanly shut down, drive registration replaces that state with
 * the startup reason and leaves the drive down. 
 * This ensures we leave previous down reasons intact instead of blindly overriding them with the startup reason.
 */
TEST_F(DriveHandlerTest, RegistrationReplacesAbsentAndCleanShutdownReasonsWithStartup) {
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

/*
 * If shutdown finds no active failure reason, it publishes a clean-shutdown reason.
 * An existing operator reason remains intact so shutdown does not erase it.
 */
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
