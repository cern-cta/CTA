/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveSession.hpp"

#include "common/exception/LostDatabaseConnection.hpp"
#include "common/exception/TimeoutException.hpp"
#include "common/log/StringLogger.hpp"
#include "common/utils/ScopeExit.hpp"
#include "runtime/config/parsing/TomlParser.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/TapeMount.hpp"
#include "session/TapeSessionWorkerTeardownIncomplete.hpp"

#include <algorithm>
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

  void complete() override { ADD_FAILURE() << "Mount completion belongs to TapeSession"; }

  void setDriveStatus(DriveStatus, const std::optional<std::string>&) override {}

  void setTapeSessionStats(const TapeTransferStats&) override {}

  void setTapeMounted(log::LogContext&) const override {}
};
}  // namespace

class DriveSessionTest : public testing::Test {
protected:
  TapedConfig config;
  log::StringLogger logger {"host", "DriveSessionTest", log::DEBUG};
  testing::StrictMock<MockDriveScheduler> scheduler;
  DesiredDriveState desired;
  std::optional<TapeDrive> reported {TapeDrive {}};
  std::stop_source stop;
  unsigned int cleanings = 0;
  unsigned int schedules = 0;
  unsigned int transfers = 0;
  unsigned int resets = 0;
  unsigned int destroyed = 0;
  bool mountAlive = false;
  std::vector<unsigned int> sleeps;
  std::vector<std::optional<std::string>> cleanedVids;
  std::vector<DriveStatus> statuses;
  std::function<bool()> clean = [] { return true; };
  std::function<std::unique_ptr<TapeMount>()> schedule;
  std::function<TapeSessionResult()> transfer = [] { return TapeSessionResult {}; };
  std::function<void()> onSleep;
  std::function<void()> onReset;
  std::function<void(DriveStatus)> onStatus;
  std::function<void()> onRead;

  class Operations final : public DriveOperations {
  public:
    explicit Operations(DriveSessionTest& fixture) : f(fixture) {}

    IScheduler& scheduler() override { return f.scheduler; }

    void resetScheduler() override {
      EXPECT_FALSE(f.mountAlive);
      ++f.resets;
      if (f.onReset) {
        f.onReset();
      }
    }

    void requestDriveDown(log::LogContext&) override { ADD_FAILURE() << "Session must not handle stop publication"; }

    std::optional<TapeDrive> getDriveState() override {
      if (f.onRead) {
        f.onRead();
      }
      return f.reported;
    }

    bool logicalLibraryExists() override {
      ADD_FAILURE() << "Library waiting belongs to TapeDaemon";
      return true;
    }

    std::pair<bool, std::optional<std::string>> probeDrive() override {
      ADD_FAILURE() << "No routine hardware probes";
      return {true, std::nullopt};
    }

    std::unique_ptr<TapeMount> getNextMount() override {
      if (++f.schedules > 10) {
        throw std::logic_error("Runaway scheduling loop");
      }
      return f.schedule ? f.schedule() : nullptr;
    }

    TapeSessionResult runTapeSession(TapeMount&) override {
      EXPECT_TRUE(f.mountAlive);
      ++f.transfers;
      return f.transfer();
    }

    std::optional<TapeSessionLivenessSnapshot> tapeSessionLiveness() const override { return std::nullopt; }

    bool clean(const std::optional<std::string>& vid, bool waitMediaInDrive) override {
      EXPECT_TRUE(waitMediaInDrive);
      ++f.cleanings;
      f.cleanedVids.push_back(vid);
      return f.clean();
    }

    void sleep(unsigned int seconds) override {
      EXPECT_FALSE(f.mountAlive);
      f.sleeps.push_back(seconds);
      if (f.sleeps.size() > 10) {
        throw std::logic_error("Runaway wait loop");
      }
      if (f.onSleep) {
        f.onSleep();
      }
    }

  private:
    DriveSessionTest& f;
  } operations {*this};

  void SetUp() override {
    config.drive.name = "drive";
    config.mounts.idle_scheduling_interval_secs = 7;
    desired.up = true;
    reported->driveStatus = DriveStatus::Down;
    EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).Times(testing::AnyNumber()).WillRepeatedly(Invoke([&] {
      return desired;
    }));
    EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, _, _))
      .Times(testing::AnyNumber())
      .WillRepeatedly(Invoke([&](const DriveInfo&, MountType, DriveStatus status, log::LogContext&) {
        statuses.push_back(status);
        if (onStatus) {
          onStatus(status);
        }
        ASSERT_TRUE(reported);
        reported->driveStatus = status;
        reported->currentVid.reset();
      }));
    EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
      .Times(testing::AnyNumber())
      .WillRepeatedly(Invoke([&](const std::string&, const DesiredDriveState& state, log::LogContext&) {
        desired.up = state.up;
        if (state.reason) {
          desired.reason = state.reason;
        }
      }));
  }

  std::unique_ptr<DriveSession> create() { return DriveSession::create(config, logger, operations); }

  std::unique_ptr<TapeMount> mount() {
    EXPECT_FALSE(mountAlive);
    mountAlive = true;
    auto result = std::make_unique<TestMount>();
    result->onDestroy = [&] {
      mountAlive = false;
      ++destroyed;
    };
    return result;
  }
};

TEST_F(DriveSessionTest, PreparesOnceAndDestructorPreservesOperatorIntent) {
  auto session = create();
  ASSERT_TRUE(session);
  EXPECT_EQ((std::vector<DriveStatus> {DriveStatus::CleaningUp, DriveStatus::Up}), statuses);
  EXPECT_FALSE(cleanedVids.front());
  desired.reason = "Pending operator request";
  EXPECT_CALL(scheduler, setDesiredDriveState(_, _, _)).Times(0);
  session.reset();
  EXPECT_EQ(DriveStatus::Down, reported->driveStatus);
  EXPECT_TRUE(desired.up);
  EXPECT_EQ("Pending operator request", desired.reason);
  EXPECT_EQ(1, cleanings);
}

TEST_F(DriveSessionTest, IgnoresCatalogueVidOnPreparation) {
  reported->currentVid = "CATALOGUE";
  ASSERT_TRUE(create());
  EXPECT_FALSE(cleanedVids.front());
}

TEST_F(DriveSessionTest, SupportsUnknownVid) {
  ASSERT_TRUE(create());
  EXPECT_FALSE(cleanedVids.front());
}

TEST_F(DriveSessionTest, RequestedDownSkipsHardware) {
  desired.up = false;
  desired.reason = "Operator maintenance";
  EXPECT_CALL(scheduler, setDesiredDriveState(_, _, _)).Times(0);
  EXPECT_FALSE(create());
  EXPECT_EQ(0, cleanings);
  EXPECT_EQ("Operator maintenance", desired.reason);
}

TEST_F(DriveSessionTest, DownDuringPreparationDoesNotPublishUp) {
  clean = [&] {
    desired.up = false;
    desired.reason = "Maintenance";
    return true;
  };
  EXPECT_FALSE(create());
  EXPECT_EQ(1, cleanings);
  EXPECT_EQ(0, std::count(statuses.begin(), statuses.end(), DriveStatus::Up));
  EXPECT_EQ("Maintenance", desired.reason);
}

TEST_F(DriveSessionTest, FailedPreparationRetainsSpecificReason) {
  clean = [&] {
    desired.up = false;
    desired.reason = "Cartridge physically stuck";
    return false;
  };
  EXPECT_FALSE(create());
  EXPECT_EQ(1, cleanings);
  EXPECT_EQ("Cartridge physically stuck", desired.reason);
  EXPECT_FALSE(desired.up);
}

TEST_F(DriveSessionTest, CleanupExceptionsAreExpectedPreparationFailures) {
  clean = []() -> bool { throw std::runtime_error("hardware failure"); };
  EXPECT_FALSE(create());
  ASSERT_TRUE(desired.reason);
  EXPECT_NE(std::string::npos, desired.reason->find("hardware failure"));
  EXPECT_EQ(1, cleanings);
}

TEST_F(DriveSessionTest, NonStandardCleanupExceptionIsContained) {
  clean = []() -> bool { throw 42; };
  EXPECT_FALSE(create());
  EXPECT_FALSE(desired.up);
  EXPECT_EQ(1, cleanings);
}

TEST_F(DriveSessionTest, ConstructorPublicationFailureRollsBackWithoutReplacingException) {
  onStatus = [](DriveStatus status) {
    if (status == DriveStatus::Up) {
      throw std::logic_error("original failure");
    }
    if (status == DriveStatus::Down) {
      throw 42;
    }
  };
  try {
    auto session = create();
    FAIL() << "Expected publication failure";
  } catch (const std::logic_error& ex) {
    EXPECT_STREQ("original failure", ex.what());
  }
  EXPECT_EQ(DriveStatus::Down, statuses.back());
  EXPECT_EQ(1, cleanings);
}

TEST_F(DriveSessionTest, MissingDrivePropagatesWithoutRecreation) {
  reported.reset();
  EXPECT_THROW(create(), Scheduler::NoSuchDrive);
  EXPECT_EQ(0, cleanings);
  EXPECT_TRUE(statuses.empty());
}

TEST_F(DriveSessionTest, ReusableSessionsAndIdlePollsDoNotCleanAgain) {
  auto session = create();
  schedule = [&]() -> std::unique_ptr<TapeMount> {
    if (schedules <= 2) {
      return mount();
    }
    return nullptr;
  };
  onSleep = [&] { stop.request_stop(); };
  session->run(stop.get_token());
  EXPECT_EQ(2, transfers);
  EXPECT_EQ(2, destroyed);
  EXPECT_EQ(3, resets);
  EXPECT_EQ((std::vector<unsigned int> {7}), sleeps);
  EXPECT_EQ(1, cleanings);
}

TEST_F(DriveSessionTest, StandardSessionExceptionRecoversBeforeDestroyingMount) {
  auto session = create();
  schedule = [&] { return mount(); };
  transfer = [&]() -> TapeSessionResult {
    if (transfers == 1) {
      throw std::runtime_error("transfer failure");
    }
    stop.request_stop();
    return {};
  };
  clean = [&] {
    EXPECT_TRUE(mountAlive);
    EXPECT_EQ(0, resets);
    return true;
  };
  session->run(stop.get_token());
  EXPECT_EQ(2, transfers);
  EXPECT_EQ(2, cleanings);
  EXPECT_EQ("V00001", cleanedVids.back());
  EXPECT_EQ(2, destroyed);
  EXPECT_EQ(2, resets);
  EXPECT_EQ((std::vector<unsigned int> {7}), sleeps);
}

TEST_F(DriveSessionTest, NonStandardSessionExceptionRecoversAndResumes) {
  auto session = create();
  schedule = [&] { return mount(); };
  transfer = [&]() -> TapeSessionResult {
    if (transfers == 1) {
      throw 42;
    }
    stop.request_stop();
    return {};
  };
  session->run(stop.get_token());
  EXPECT_EQ(2, transfers);
  EXPECT_EQ(2, cleanings);
  EXPECT_EQ(2, resets);
}

TEST_F(DriveSessionTest, FailedRecoveryEndsSchedulingAndRequiresNewUpRequest) {
  auto session = create();
  schedule = [&] { return mount(); };
  transfer = []() -> TapeSessionResult { throw std::runtime_error("transfer"); };
  clean = [&] {
    desired.up = false;
    desired.reason = "Stuck tape";
    return false;
  };
  session->run(stop.get_token());
  session.reset();
  EXPECT_FALSE(create());
  EXPECT_EQ(1, transfers);
  EXPECT_EQ(2, cleanings);
  EXPECT_EQ("Stuck tape", desired.reason);
  EXPECT_TRUE(sleeps.empty());
  EXPECT_EQ("V00001", cleanedVids.back());
  reported->currentVid = "STALE";
  desired.up = true;
  clean = [] { return true; };
  EXPECT_TRUE(create());
  EXPECT_FALSE(cleanedVids.back());
}

TEST_F(DriveSessionTest, RecoveryExceptionPutsDriveDown) {
  auto session = create();
  schedule = [&] { return mount(); };
  transfer = []() -> TapeSessionResult { throw 42; };
  clean = []() -> bool { throw std::runtime_error("recovery failure"); };
  session->run(stop.get_token());
  EXPECT_FALSE(desired.up);
  ASSERT_TRUE(desired.reason);
  EXPECT_NE(std::string::npos, desired.reason->find("recovery failure"));
  EXPECT_EQ(1, transfers);
  EXPECT_EQ(2, cleanings);
}

TEST_F(DriveSessionTest, DownAfterSessionFailureDefersCleanupWithUnknownVid) {
  auto session = create();
  schedule = [&] { return mount(); };
  transfer = [&]() -> TapeSessionResult {
    desired.up = false;
    desired.reason = "Maintenance";
    throw 42;
  };
  session->run(stop.get_token());
  session.reset();
  EXPECT_EQ(1, cleanings);
  EXPECT_EQ("Maintenance", desired.reason);
  reported->currentVid = "EXTERNALLY_CHANGED";
  desired.up = true;
  ASSERT_TRUE(create());
  EXPECT_FALSE(cleanedVids.back());
}

TEST_F(DriveSessionTest, DownDuringRecoveryPreventsFurtherScheduling) {
  auto session = create();
  schedule = [&] { return mount(); };
  transfer = []() -> TapeSessionResult { throw 42; };
  clean = [&] {
    desired.up = false;
    return true;
  };
  session->run(stop.get_token());
  EXPECT_EQ(1, transfers);
  EXPECT_EQ(2, cleanings);
  EXPECT_EQ(DriveStatus::Down, reported->driveStatus);
}

TEST_F(DriveSessionTest, IncompleteWorkerTeardownRemainsFatal) {
  auto session = create();
  schedule = [&] { return mount(); };
  transfer = []() -> TapeSessionResult { throw TapeSessionWorkerTeardownIncomplete(); };
  EXPECT_THROW(session->run(stop.get_token()), TapeSessionWorkerTeardownIncomplete);
  session.reset();
  EXPECT_EQ(1, cleanings);
  EXPECT_EQ(0, resets);
  EXPECT_EQ(1, destroyed);
}

TEST_F(DriveSessionTest, UnusableResultDoesNotRetryCleanup) {
  auto session = create();
  schedule = [&] { return mount(); };
  transfer = [&] {
    desired.up = false;
    desired.reason = "Specific session failure";
    return TapeSessionResult {.driveReusable = false, .successful = false};
  };
  session->run(stop.get_token());
  EXPECT_EQ(1, cleanings);
  EXPECT_EQ(1, transfers);
  EXPECT_EQ("Specific session failure", desired.reason);
}

TEST_F(DriveSessionTest, ReportedDownWithNewUpRequestEndsPeriodAndPreparesAgain) {
  auto session = create();
  reported->driveStatus = DriveStatus::Down;
  session->run(stop.get_token());
  session.reset();
  EXPECT_EQ(0, schedules);
  EXPECT_TRUE(desired.up);
  EXPECT_TRUE(create());
  EXPECT_EQ(2, cleanings);
}

TEST_F(DriveSessionTest, DesiredDownEndsPeriodWithoutHardwareAccess) {
  auto session = create();
  desired.up = false;
  session->run(stop.get_token());
  session.reset();
  EXPECT_EQ(0, schedules);
  EXPECT_EQ(1, cleanings);
}

TEST_F(DriveSessionTest, StopBeforeRunPreventsScheduling) {
  auto session = create();
  stop.request_stop();
  session->run(stop.get_token());
  EXPECT_EQ(0, schedules);
  EXPECT_EQ(0, resets);
}

TEST_F(DriveSessionTest, StopDuringSchedulingDestroysMountWithoutStartingTransfer) {
  auto session = create();
  schedule = [&] {
    stop.request_stop();
    return mount();
  };
  session->run(stop.get_token());
  EXPECT_EQ(0, transfers);
  EXPECT_EQ(1, destroyed);
  EXPECT_EQ(1, resets);
  EXPECT_TRUE(sleeps.empty());
}

TEST_F(DriveSessionTest, SchedulerResetStopPreventsRetrySleep) {
  auto session = create();
  onReset = [&] { stop.request_stop(); };
  session->run(stop.get_token());
  EXPECT_EQ(1, schedules);
  EXPECT_TRUE(sleeps.empty());
}

TEST_F(DriveSessionTest, SchedulingTimeoutRetriesWithoutCleanup) {
  auto session = create();
  schedule = []() -> std::unique_ptr<TapeMount> { throw exception::TimeoutException("timeout"); };
  onSleep = [&] { stop.request_stop(); };
  session->run(stop.get_token());
  EXPECT_EQ(1, cleanings);
  EXPECT_EQ(1, resets);
  EXPECT_EQ((std::vector<unsigned int> {7}), sleeps);
}

TEST_F(DriveSessionTest, DestructorContainsPublicationFailure) {
  auto session = create();
  onStatus = [](DriveStatus) { throw std::runtime_error("database unavailable"); };
  EXPECT_NO_THROW(session.reset());
  EXPECT_EQ(1, cleanings);
  EXPECT_TRUE(desired.up);
}

TEST_F(DriveSessionTest, DestructorContainsNonStandardStateReadFailure) {
  auto session = create();
  onRead = [] { throw 42; };
  EXPECT_NO_THROW(session.reset());
  EXPECT_EQ(1, cleanings);
}

TEST_F(DriveSessionTest, DestructorDoesNotPublishForMissingDrive) {
  auto session = create();
  reported.reset();
  statuses.clear();
  EXPECT_NO_THROW(session.reset());
  EXPECT_TRUE(statuses.empty());
  EXPECT_EQ(1, cleanings);
}

}  // namespace cta::tape::daemon
