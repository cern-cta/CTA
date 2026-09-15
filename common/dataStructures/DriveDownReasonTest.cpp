/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include "DriveDownReason.hpp"

#include "common/log/Logger.hpp"

#include <gtest/gtest.h>

namespace cta::common::dataStructures {
TEST(DriveDownReasonTest, FormatsSeverityAndDetails) {
  EXPECT_EQ(log::INFO, driveDownReasonSeverity(DriveDownReason::Startup));
  EXPECT_EQ(log::INFO, driveDownReasonSeverity(DriveDownReason::Shutdown));
  EXPECT_EQ(log::ERR, driveDownReasonSeverity(DriveDownReason::DriveOpenFailed));
  EXPECT_EQ(log::ERR, driveDownReasonSeverity(DriveDownReason::DriveProbeFailed));
  EXPECT_EQ("[cta-taped] ERROR Drive probe failed: device unavailable",
            formatDriveDownReason(DriveDownReason::DriveProbeFailed, "device unavailable"));
  EXPECT_EQ("[cta-taped] INFO Startup", formatDriveDownReason(DriveDownReason::Startup));
  EXPECT_EQ("[cta-taped] ERROR Drive open failed: permission denied",
            formatDriveDownReason(DriveDownReason::DriveOpenFailed, "permission denied"));
}

TEST(DriveDownReasonTest, RecognizesCurrentAndLegacyShutdownReasons) {
  EXPECT_TRUE(isCleanDriveShutdownReason(formatDriveDownReason(DriveDownReason::Shutdown)));
  for (const auto* reason : {"[cta-taped] Exiting cta-taped",
                             "[cta-taped] INFO Exiting cta-taped",
                             "[cta-taped] ERROR Exiting cta-taped",
                             "[cta-taped] ERROR [cta-taped] Exiting cta-taped"}) {
    EXPECT_TRUE(isCleanDriveShutdownReason(reason)) << reason;
  }
}

TEST(DriveDownReasonTest, DoesNotClassifyOtherReasonsAsCleanShutdown) {
  EXPECT_FALSE(isCleanDriveShutdownReason(""));
  EXPECT_FALSE(isCleanDriveShutdownReason("Operator requested maintenance"));
  EXPECT_FALSE(isCleanDriveShutdownReason("[cta-taped] INFO Shutdown: operator detail"));
  EXPECT_FALSE(isCleanDriveShutdownReason(formatDriveDownReason(DriveDownReason::Startup)));
  EXPECT_FALSE(isCleanDriveShutdownReason(formatDriveDownReason(DriveDownReason::CleanerFailed)));
}
}  // namespace cta::common::dataStructures
