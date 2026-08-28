/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/TapeDrivesCatalogueState.hpp"

#include "catalogue/dummy/DummyCatalogue.hpp"
#include "common/dataStructures/DesiredDriveState.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/StringLogger.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <stdexcept>

namespace cta {
namespace {
using namespace common::dataStructures;
using testing::Field;
using testing::Invoke;
using testing::Return;
using testing::Throw;

class RequestDriveState : public catalogue::DummyDriveStateCatalogue {
public:
  MOCK_METHOD(std::optional<TapeDrive>, getTapeDrive, (const std::string&), (const, override));
  MOCK_METHOD(void, setDesiredTapeDriveState, (const std::string&, const DesiredDriveState&), (override));
  MOCK_METHOD(void, setDesiredTapeDriveStateComment, (const std::string&, const std::string&), (override));
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

TEST_F(TapeDrivesCatalogueStateTest, OperatorUpClearsForceAndExplicitlyReplacesReason) {
  for (const auto reason : {"", "Ready after maintenance"}) {
    EXPECT_CALL(catalogue.driveState(), setDesiredTapeDriveState("drive", testing::_))
      .WillOnce(Invoke([&](const auto&, const DesiredDriveState& desired) {
        EXPECT_TRUE(desired.up);
        EXPECT_FALSE(desired.forceDown);
        EXPECT_EQ(reason, desired.reason);
        EXPECT_FALSE(desired.comment);
      }));
    state.requestDriveUp("drive", reason, lc);
  }
}

TEST_F(TapeDrivesCatalogueStateTest, OperatorDownReplacesReasonAndPreservesForceRequest) {
  TapeDrive drive;
  drive.desiredUp = true;
  EXPECT_CALL(catalogue.driveState(), getTapeDrive("drive")).WillOnce(Return(drive));
  EXPECT_CALL(catalogue.driveState(), setDesiredTapeDriveState("drive", testing::_))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& desired) {
      EXPECT_FALSE(desired.up);
      EXPECT_TRUE(desired.forceDown);
      EXPECT_EQ("Operator maintenance", desired.reason);
      EXPECT_FALSE(desired.comment);
    }));
  // Explicit commands do not read and preserve a previous reason.
  state.requestDriveDown("drive", {.reason = "Operator maintenance", .force = true}, lc);
  EXPECT_THAT(logger.getLog(), testing::HasSubstr("Putting the drive down"));
  EXPECT_THAT(logger.getLog(), testing::HasSubstr("putting_tape_drive_down"));
  EXPECT_THAT(logger.getLog(), testing::HasSubstr("drive_name=\"drive\""));
}

TEST_F(TapeDrivesCatalogueStateTest, AlreadyDownStillUpdatesOperatorReasonAndForceWithoutAnotherEvent) {
  TapeDrive drive;
  drive.desiredUp = false;
  EXPECT_CALL(catalogue.driveState(), getTapeDrive("drive")).WillOnce(Return(drive));
  EXPECT_CALL(catalogue.driveState(), setDesiredTapeDriveState("drive", testing::_))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& desired) {
      EXPECT_FALSE(desired.up);
      EXPECT_TRUE(desired.forceDown);
      EXPECT_EQ("Operator maintenance", desired.reason);
    }));
  state.requestDriveDown("drive", {.reason = "Operator maintenance", .force = true}, lc);
  EXPECT_EQ(std::string::npos, logger.getLog().find("putting_tape_drive_down"));
  EXPECT_EQ(std::string::npos, logger.getLog().find("Putting the drive down"));
}

TEST_F(TapeDrivesCatalogueStateTest, CommentOnlyUpdatesCommentAndDoesNotLogAStateChange) {
  EXPECT_CALL(catalogue.driveState(), setDesiredTapeDriveStateComment("drive", "Inspect cabling"));
  state.setDriveComment("drive", "Inspect cabling", lc);
  EXPECT_EQ(std::string::npos, logger.getLog().find("Putting the drive"));
}

TEST_F(TapeDrivesCatalogueStateTest, PreservedReasonIsOmittedFromUpdate) {
  TapeDrive drive;
  drive.desiredUp = false;
  drive.reasonUpDown = "Operator maintenance";
  EXPECT_CALL(catalogue.driveState(), getTapeDrive("drive")).WillOnce(Return(drive));
  EXPECT_CALL(catalogue.driveState(), setDesiredTapeDriveState("drive", Field(&DesiredDriveState::up, false)))
    .WillOnce(Invoke([](const auto&, const DesiredDriveState& desired) {
      EXPECT_FALSE(desired.up);
      // Do not write the snapshot's reason over a concurrent operator update.
      EXPECT_FALSE(desired.reason);
    }));

  state.requestDriveDown("drive",
                         cta::common::dataStructures::DriveDownRequest::forDaemon(DriveDownReason::Shutdown),
                         lc);
  EXPECT_THAT(logger.getLog(), testing::HasSubstr("reasonPreserved=\"1\""));
  EXPECT_EQ(std::string::npos, logger.getLog().find("reason=\"\""));
  EXPECT_EQ(std::string::npos, logger.getLog().find("Putting the drive down"));
  EXPECT_EQ(std::string::npos, logger.getLog().find("putting_tape_drive_down"));
}

TEST_F(TapeDrivesCatalogueStateTest, LookupFailurePropagatesWithoutPublication) {
  EXPECT_CALL(catalogue.driveState(), getTapeDrive("drive")).WillOnce(Throw(std::runtime_error("lookup failed")));

  EXPECT_THROW(
    state.requestDriveDown("drive",
                           cta::common::dataStructures::DriveDownRequest::forDaemon(DriveDownReason::Shutdown),
                           lc),
    std::runtime_error);
}

TEST_F(TapeDrivesCatalogueStateTest, PublicationFailurePropagates) {
  EXPECT_CALL(catalogue.driveState(), getTapeDrive("drive")).WillOnce(Return(TapeDrive {}));
  EXPECT_CALL(catalogue.driveState(), setDesiredTapeDriveState("drive", Field(&DesiredDriveState::up, false)))
    .WillOnce(Throw(std::runtime_error("publication failed")));

  EXPECT_THROW(
    state.requestDriveDown("drive",
                           cta::common::dataStructures::DriveDownRequest::forDaemon(DriveDownReason::Shutdown),
                           lc),
    std::runtime_error);
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
    EXPECT_CALL(catalogue.driveState(), setDesiredTapeDriveState("drive", Field(&DesiredDriveState::up, false)))
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
    state.requestDriveDown(
      "drive",
      cta::common::dataStructures::DriveDownRequest::forDaemon(DriveDownReason::DriveCleanupFailed, "detail"),
      lc);
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(&catalogue.driveState()));
  }
}

TEST_F(TapeDrivesCatalogueStateTest, ReportsLegacyActivityAndLeavesUnrepresentablePhasesUnchanged) {
  using enum common::dataStructures::TapeSessionState;
  const DriveInfo info {"drive", "host", "library", "device", "slot"};
  const std::pair<TapeSessionState, DriveStatus> phases[] {
    {Preparing,      DriveStatus::Starting      },
    {Mounting,       DriveStatus::Mounting      },
    {Transferring,   DriveStatus::Transferring  },
    {Unloading,      DriveStatus::Unloading     },
    {Unmounting,     DriveStatus::Unmounting    },
    {DrainingToDisk, DriveStatus::DrainingToDisk}
  };
  for (const auto readiness : {ReportedDriveState::Preparing, ReportedDriveState::Up}) {
    for (const auto& [phase, status] : phases) {
      EXPECT_CALL(catalogue.driveState(), updateTapeDriveStatus(Field(&TapeDrive::driveStatus, status)))
        .WillOnce(Return(true));
      state.reportDriveState(info, {.state = readiness, .tapeSession = TapeSessionReport {.state = phase}}, lc);
    }
    // Strict mocks reject any write for these local-only phases.
    for (const auto phase : {Loading, Finalizing, Finished}) {
      state.reportDriveState(info, {.state = readiness, .tapeSession = TapeSessionReport {.state = phase}}, lc);
    }
  }
  EXPECT_CALL(catalogue.driveState(), updateTapeDriveStatus(Field(&TapeDrive::driveStatus, DriveStatus::Down)))
    .WillOnce(Return(true));
  state.reportDriveState(info, {.state = ReportedDriveState::Down}, lc);
  EXPECT_CALL(catalogue.driveState(), updateTapeDriveStatus(Field(&TapeDrive::driveStatus, DriveStatus::Up)))
    .WillOnce(Return(true));
  state.reportDriveState(info, {.state = ReportedDriveState::Up}, lc);
}

TEST_F(TapeDrivesCatalogueStateTest, PreparationWithoutTapeSessionPublishesStarting) {
  const DriveInfo info {"drive", "host", "library", "device", "slot"};
  EXPECT_CALL(catalogue.driveState(),
              updateTapeDriveStatus(testing::AllOf(Field(&TapeDrive::driveStatus, DriveStatus::Starting),
                                                   Field(&TapeDrive::mountType, MountType::NoMount))))
    .WillOnce(Return(true));
  state.reportDriveState(info, {.state = ReportedDriveState::Preparing}, lc);
}

TEST_F(TapeDrivesCatalogueStateTest, DownRejectsTapeActivity) {
  const DriveInfo info {"drive", "host", "library", "device", "slot"};
  const DriveReport report {.state = ReportedDriveState::Down,
                            .tapeSession = TapeSessionReport {.state = TapeSessionState::Unloading}};
  EXPECT_THROW(state.reportDriveState(info, report, lc), exception::Exception);
}

TEST_F(TapeDrivesCatalogueStateTest, ReportPreservesSessionMetadataWithoutChangingDesiredState) {
  const DriveInfo driveInfo {"drive", "host", "library", "/dev/tape", "slot"};
  const DriveReport report {
    .state = ReportedDriveState::Up,
    .tapeSession = TapeSessionReport {.state = TapeSessionState::Transferring,
                                      .mountType = MountType::Retrieve,
                                      .mountSessionId = 42,
                                      .bytesTransferred = 1024,
                                      .filesTransferred = 2,
                                      .vid = "V00001",
                                      .tapePool = "pool",
                                      .vo = "vo",
                                      .priority = 7,
                                      .activity = "activity"},
    .reportTime = 1234
  };
  EXPECT_CALL(catalogue.driveState(), updateTapeDriveStatus(testing::_)).WillOnce(Invoke([](const TapeDrive& drive) {
    EXPECT_EQ(DriveStatus::Transferring, drive.driveStatus);
    EXPECT_EQ(MountType::Retrieve, drive.mountType);
    EXPECT_EQ(42, drive.sessionId);
    EXPECT_EQ(1024, drive.bytesTransferedInSession);
    EXPECT_EQ(2, drive.filesTransferedInSession);
    EXPECT_EQ("V00001", drive.currentVid);
    EXPECT_EQ("pool", drive.currentTapePool);
    EXPECT_EQ("vo", drive.currentVo);
    EXPECT_EQ(7, drive.currentPriority);
    EXPECT_EQ("activity", drive.currentActivity);
    EXPECT_EQ(1234, drive.transferStartTime);
    return true;
  }));

  // Strict mocks also reject an accidental write to desired state.
  state.reportDriveState(driveInfo, report, lc);
}

}  // namespace cta
