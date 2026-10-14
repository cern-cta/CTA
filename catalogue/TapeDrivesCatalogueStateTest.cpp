/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/TapeDrivesCatalogueState.hpp"

#include "catalogue/dummy/DummyCatalogue.hpp"
#include "common/dataStructures/DesiredDriveState.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/StringLogger.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <stdexcept>

namespace cta {
namespace {
using namespace common::dataStructures;
using testing::_;
using testing::Invoke;
using testing::Return;
using testing::Throw;

class RequestDriveState : public catalogue::DummyDriveStateCatalogue {
public:
  MOCK_METHOD(std::optional<TapeDrive>, getTapeDrive, (const std::string&), (const, override));
  MOCK_METHOD(void, setDesiredTapeDriveState, (const std::string&, const DesiredDriveState&), (override));
  MOCK_METHOD(bool, updateTapeDriveStatus, (const TapeDrive&), (override));
};

class RequestCatalogue : public catalogue::DummyCatalogue {
public:
  RequestCatalogue() { m_driveState = std::make_unique<testing::StrictMock<RequestDriveState>>(); }

  RequestDriveState& driveState() { return static_cast<RequestDriveState&>(*m_driveState); }
};
}  // namespace

class TapeDrivesCatalogueStateTest : public testing::Test {
protected:
  RequestCatalogue catalogue;
  TapeDrivesCatalogueState state {catalogue};
  log::StringLogger logger {"host", "TapeDrivesCatalogueStateTest", log::DEBUG};
  log::LogContext lc {logger};
};

TEST_F(TapeDrivesCatalogueStateTest, DesiredStatePreservesAllOperatorFields) {
  TapeDrive drive;
  drive.desiredUp = true;
  drive.desiredForceDown = true;
  drive.reasonUpDown = "Operator request";
  drive.userComment = "Inspect drive";
  EXPECT_CALL(catalogue.driveState(), getTapeDrive("drive")).WillOnce(Return(drive));

  const auto desired = state.getDesiredDriveState("drive");
  EXPECT_EQ(drive.desiredUp, desired.up);
  EXPECT_EQ(drive.desiredForceDown, desired.forceDown);
  EXPECT_EQ(drive.reasonUpDown, desired.reason);
  EXPECT_EQ(drive.userComment, desired.comment);
}

TEST_F(TapeDrivesCatalogueStateTest, MissingDesiredStateThrows) {
  EXPECT_CALL(catalogue.driveState(), getTapeDrive("drive")).WillOnce(Return(std::nullopt));
  EXPECT_THROW(state.getDesiredDriveState("drive"), TapeDrivesCatalogueState::NoSuchDrive);
}

TEST_F(TapeDrivesCatalogueStateTest, BackendNameIsCreatedThenUpdated) {
  state.reportSchedulerBackendName("drive", "first", lc);
  state.reportSchedulerBackendName("drive", "replacement", lc);

  const auto configs = catalogue.DriveConfig()->getTapeDriveConfigs();
  ASSERT_EQ(1, configs.size());
  EXPECT_EQ("drive", configs.front().tapeDriveName);
  EXPECT_EQ("SchedulerBackendName", configs.front().keyName);
  EXPECT_EQ("replacement", configs.front().value);
  EXPECT_EQ("TOML config file", configs.front().source);
}

TEST_F(TapeDrivesCatalogueStateTest, PreservedReasonIsOmittedFromUpdate) {
  TapeDrive drive;
  drive.desiredUp = false;
  drive.reasonUpDown = "Operator maintenance";
  EXPECT_CALL(catalogue.driveState(), getTapeDrive("drive")).WillOnce(Return(drive));
  EXPECT_CALL(catalogue.driveState(), setDesiredTapeDriveState("drive", _))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& desired) {
      EXPECT_FALSE(desired.up);
      // Do not write the snapshot's reason over a concurrent operator update.
      EXPECT_FALSE(desired.reason);
    }));

  state.requestDriveDown("drive", DriveDownReason::Shutdown, lc);
}

TEST_F(TapeDrivesCatalogueStateTest, LookupFailurePropagatesWithoutPublication) {
  EXPECT_CALL(catalogue.driveState(), getTapeDrive("drive")).WillOnce(Throw(std::runtime_error("lookup failed")));

  EXPECT_THROW(state.requestDriveDown("drive", DriveDownReason::Shutdown, lc), std::runtime_error);
}

TEST_F(TapeDrivesCatalogueStateTest, PublicationFailurePropagates) {
  EXPECT_CALL(catalogue.driveState(), getTapeDrive("drive")).WillOnce(Return(TapeDrive {}));
  EXPECT_CALL(catalogue.driveState(), setDesiredTapeDriveState("drive", _))
    .WillOnce(Throw(std::runtime_error("publication failed")));

  EXPECT_THROW(state.requestDriveDown("drive", DriveDownReason::Shutdown, lc), std::runtime_error);
}

TEST_F(TapeDrivesCatalogueStateTest, DownReasonMessageControlsReplacement) {
  struct Case {
    const char* reason;
    bool replace;
  };

  const Case cases[] {
    {"[cta-taped] INFO Startup",                                                          false},
    {"[cta-taped] INFO Shutdown",                                                         false},
    {"[cta-taped] Daemon started",                                                        true },
    {"[cta-taped] Daemon shutting down: SIGTERM",                                         true },
    {"[cta-taped] INFO Shutdown: inspect drive",                                          false},
    {"[cta-taped:drive-cleanup-failed] 2020-01-01T00:00:00Z ERROR Stuck tape",            false},
    {"[cta-taped:future-code] 2020-01-01T00:00:00Z INFO Shutdown",                        false},
    {"[cta-taped] Daemon shutting down",                                                  true },
    {"[cta-taped] Daemon started: desired Up detected, but startup recovery is disabled", false},
    {"Operator maintenance",                                                              false}
  };
  for (const auto& test : cases) {
    SCOPED_TRACE(test.reason);
    TapeDrive drive;
    drive.desiredUp = false;
    drive.reasonUpDown = test.reason;
    EXPECT_CALL(catalogue.driveState(), getTapeDrive("drive")).WillOnce(Return(drive));
    EXPECT_CALL(catalogue.driveState(), setDesiredTapeDriveState("drive", _))
      .WillOnce(Invoke([&](const auto&, const DesiredDriveState& desired) {
        EXPECT_FALSE(desired.up);
        if (test.replace) {
          ASSERT_TRUE(desired.reason);
          EXPECT_EQ("[cta-taped] Drive cleanup failed: detail", *desired.reason);
        } else {
          // Omitting the field preserves the original text.
          EXPECT_FALSE(desired.reason);
        }
      }));
    state.requestDriveDown("drive", DriveDownReason::DriveCleanupFailed, lc, "detail");
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&catalogue.driveState()));
  }
}

}  // namespace cta
