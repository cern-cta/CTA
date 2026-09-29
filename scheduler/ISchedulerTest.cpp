/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "IScheduler.hpp"

#include "common/dataStructures/DesiredDriveState.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/StringLogger.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <stdexcept>
#include <vector>

namespace cta {
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

}  // namespace

class ISchedulerTest : public testing::Test {
protected:
  testing::StrictMock<MockDriveScheduler> scheduler;
  log::StringLogger logger {"host", "ISchedulerTest", log::DEBUG};
  log::LogContext lc {logger};
  DriveInfo drive {"drive", "host", "library", "device", "slot"};
};

TEST_F(ISchedulerTest, PreservesSpecificReasonsAndReplacesAbsentOrCleanReasons) {
  const std::vector<std::optional<std::string>> reasons {std::nullopt,
                                                         "",
                                                         formatDriveDownReason(DriveDownReason::Startup),
                                                         formatDriveDownReason(DriveDownReason::Shutdown),
                                                         "Operator maintenance",
                                                         "Cartridge stuck"};
  for (const bool up : {false, true}) {
    for (size_t i = 0; i < reasons.size(); ++i) {
      SCOPED_TRACE(reasons[i].value_or("absent"));
      SCOPED_TRACE(up);
      testing::InSequence sequence;
      DesiredDriveState current;
      current.up = up;
      current.reason = reasons[i];
      EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Return(current));
      EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _));
      EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
        .WillOnce(Invoke([&](const auto&, const DesiredDriveState& desired, auto&) {
          EXPECT_FALSE(desired.up);
          if (!up && i >= 4) {
            EXPECT_FALSE(desired.reason);  // An absent update leaves the catalogue reason untouched.
          } else {
            EXPECT_EQ(formatDriveDownReason(DriveDownReason::DriveCleanupFailed, "detail"), desired.reason);
          }
        }));
      scheduler.putDriveDown(drive, DriveDownReason::DriveCleanupFailed, lc, "detail");
      ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&scheduler));
    }
  }
}

TEST_F(ISchedulerTest, AttemptsBothPublicationsAndRethrowsFirstFailure) {
  for (const bool failLookup : {false, true}) {
    for (const bool failReported : {false, true}) {
      for (const bool failDesired : {false, true}) {
        SCOPED_TRACE(failLookup);
        SCOPED_TRACE(failReported);
        SCOPED_TRACE(failDesired);
        testing::InSequence sequence;
        EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Invoke([&] {
          if (failLookup) {
            throw std::runtime_error("lookup");
          }
          return DesiredDriveState {};
        }));
        EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _)).WillOnce(Invoke([&] {
          if (failReported) {
            throw std::runtime_error("reported");
          }
        }));
        EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _))
          .WillOnce(Invoke([&](const auto&, const DesiredDriveState& desired, auto&) {
            EXPECT_FALSE(desired.up);
            if (failLookup) {
              EXPECT_FALSE(desired.reason);
            }
            if (failDesired) {
              throw std::runtime_error("desired");
            }
          }));
        try {
          scheduler.putDriveDown(drive, DriveDownReason::Shutdown, lc);
          EXPECT_FALSE(failLookup || failReported || failDesired);
        } catch (const std::runtime_error& ex) {
          EXPECT_STREQ(failLookup ? "lookup" : failReported ? "reported" : "desired", ex.what());
        }
        ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&scheduler));
      }
    }
  }
}

TEST_F(ISchedulerTest, NonStandardFailureDoesNotPreventRemainingPublications) {
  testing::InSequence sequence;
  EXPECT_CALL(scheduler, getDesiredDriveState("drive", _)).WillOnce(Throw(42));
  EXPECT_CALL(scheduler, reportDriveStatus(_, MountType::NoMount, DriveStatus::Down, _)).WillOnce(Throw(43));
  EXPECT_CALL(scheduler, setDesiredDriveState("drive", _, _)).WillOnce(Throw(44));
  try {
    scheduler.putDriveDown(drive, DriveDownReason::Shutdown, lc);
    FAIL() << "Expected original failure";
  } catch (int failure) {
    EXPECT_EQ(42, failure);
  }
}

}  // namespace cta
