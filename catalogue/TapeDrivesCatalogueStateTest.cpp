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

}  // namespace cta
