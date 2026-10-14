/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveStartupPolicy.hpp"

#include "TapedConfig.hpp"
#include "common/dataStructures/DriveDownReason.hpp"

#include <array>
#include <gtest/gtest.h>

namespace cta::tape::daemon {
namespace {
using common::dataStructures::DriveStatus;
using common::dataStructures::TapeDrive;
using Action = DriveStartupDecision::Action;

TEST(DriveStartupPolicyTest, NewDriveUsesAutoUpRegardlessOfRecoverySetting) {
  for (const bool recover : {false, true}) {
    for (const bool autoUp : {false, true}) {
      SCOPED_TRACE(recover);
      SCOPED_TRACE(autoUp);
      const DriveStartupConfig config {.recover_existing_up = recover, .auto_up = autoUp};
      const auto decision = decideDriveStartup(config, std::nullopt);

      EXPECT_EQ(Action::Register, decision.action);
      EXPECT_EQ(autoUp, decision.desiredState.up);
      EXPECT_FALSE(decision.desiredState.forceDown);
      EXPECT_EQ(common::dataStructures::formatDriveDownReason(common::dataStructures::DriveDownReason::Startup),
                decision.desiredState.reason);
      EXPECT_FALSE(decision.desiredState.comment);
    }
  }
}

TEST(DriveStartupPolicyTest, ExistingUpUsesRecoverySettingBeforeAutoUp) {
  std::optional<TapeDrive> previous {TapeDrive {}};
  previous->desiredUp = true;
  previous->reasonUpDown = "Operator requested Up";
  previous->userComment = "Inspect drive";

  for (const auto status : {DriveStatus::Down, DriveStatus::Transferring}) {
    previous->driveStatus = status;
    for (const bool autoUp : {false, true}) {
      SCOPED_TRACE(static_cast<int>(status));
      SCOPED_TRACE(autoUp);
      EXPECT_EQ(Action::RecoverExistingUp,
                decideDriveStartup({.recover_existing_up = true, .auto_up = autoUp}, previous).action);
      EXPECT_EQ(Action::RequireOperatorUp,
                decideDriveStartup({.recover_existing_up = false, .auto_up = autoUp}, previous).action);
    }
  }
  EXPECT_TRUE(previous->desiredUp);
  EXPECT_EQ("Operator requested Up", previous->reasonUpDown);
  EXPECT_EQ("Inspect drive", previous->userComment);
}

TEST(DriveStartupPolicyTest, ExistingDownPreservesSpecificReasonsAndComments) {
  struct Case {
    std::optional<std::string> reason;
    std::string expectedReason;
    // Expected desired Up with auto_up disabled and enabled, respectively.
    std::array<bool, 2> expectedUp;
  };

  const auto startupReason =
    common::dataStructures::formatDriveDownReason(common::dataStructures::DriveDownReason::Startup);
  const Case cases[] {
    {"[cta-taped] Daemon shutting down: SIGTERM",                                         startupReason,                              {false, true} },
    {"[cta-taped] Daemon started",                                                        "[cta-taped] Daemon started",               {false, false}},
    {"[cta-taped] Daemon shutting down",                                                  startupReason,                              {false, true} },
    {std::nullopt,                                                                        startupReason,                              {false, true} },
    {"",                                                                                  startupReason,                              {false, true} },
    {"[cta-taped] INFO Shutdown",                                                         "[cta-taped] INFO Shutdown",                {false, false}},
    {"Maintenance",                                                                       "Maintenance",                              {false, false}},
    {"[cta-taped] ERR Drive cleanup failed",                                              "[cta-taped] ERR Drive cleanup failed",     {false, false}},
    {"[cta-taped] INFO Startup",                                                          "[cta-taped] INFO Startup",                 {false, false}},
    {"[cta-taped] Daemon started: desired Up detected, but startup recovery is disabled",
     "[cta-taped] Daemon started: desired Up detected, but startup recovery is disabled",                                             {false, false}},
    {"[cta-taped] INFO Shutdown: inspect drive",                                          "[cta-taped] INFO Shutdown: inspect drive", {false, false}}
  };

  for (const auto& test : cases) {
    SCOPED_TRACE(test.reason.value_or("absent"));
    for (const bool recover : {false, true}) {
      for (size_t autoUp = 0; autoUp < test.expectedUp.size(); ++autoUp) {
        SCOPED_TRACE(recover);
        SCOPED_TRACE(autoUp);
        std::optional<TapeDrive> previous {TapeDrive {}};
        previous->desiredUp = false;
        previous->reasonUpDown = test.reason;
        previous->userComment = "Operator comment";
        const DriveStartupConfig config {.recover_existing_up = recover, .auto_up = autoUp != 0};
        const auto decision = decideDriveStartup(config, previous);

        EXPECT_EQ(Action::Register, decision.action);
        EXPECT_EQ(test.expectedUp[autoUp], decision.desiredState.up);
        EXPECT_FALSE(decision.desiredState.forceDown);
        EXPECT_EQ(test.expectedReason, decision.desiredState.reason);
        EXPECT_EQ("Operator comment", decision.desiredState.comment);
        EXPECT_EQ(test.reason, previous->reasonUpDown);
      }
    }
  }
}

}  // namespace
}  // namespace cta::tape::daemon
