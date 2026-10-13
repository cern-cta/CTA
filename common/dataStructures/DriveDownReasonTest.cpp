/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include "DriveDownReason.hpp"

#include "common/log/Logger.hpp"

#include <gtest/gtest.h>
#include <stdexcept>
#include <string_view>
#include <tuple>

namespace cta::common::dataStructures {
TEST(DriveDownReasonTest, FormatsSeverityAndDetails) {
  for (const auto& [reason, severity, prefix] : {
         std::tuple {DriveDownReason::Startup,            log::INFO, "[cta-taped] INFO " },
         std::tuple {DriveDownReason::DriveCleanupFailed, log::ERR,  "[cta-taped] ERROR "}
  }) {
    SCOPED_TRACE(static_cast<int>(reason));
    const auto formatted = formatDriveDownReason(reason);
    EXPECT_EQ(severity, driveDownReasonSeverity(reason));
    EXPECT_TRUE(formatted.starts_with(prefix));
    EXPECT_GT(formatted.size(), std::string_view(prefix).size());
    EXPECT_EQ(formatted + ": device unavailable", formatDriveDownReason(reason, "device unavailable"));
  }
}

TEST(DriveDownReasonTest, RejectsUnknownReasons) {
  const auto unknownReason = static_cast<DriveDownReason>(-1);
  EXPECT_THROW(driveDownReasonSeverity(unknownReason), std::invalid_argument);
  EXPECT_THROW(formatDriveDownReason(unknownReason), std::invalid_argument);
}

TEST(DriveDownReasonTest, RecognizesCanonicalShutdownReason) {
  EXPECT_TRUE(isCleanDriveShutdownReason("[cta-taped] INFO Shutdown"));
  EXPECT_TRUE(isCleanDriveShutdownReason(formatDriveDownReason(DriveDownReason::Shutdown)));
}

TEST(DriveDownReasonTest, DoesNotClassifyOtherReasonsAsCleanShutdown) {
  EXPECT_FALSE(isCleanDriveShutdownReason(""));
  EXPECT_FALSE(isCleanDriveShutdownReason("Operator requested maintenance"));
  EXPECT_FALSE(isCleanDriveShutdownReason("[cta-taped] INFO Shutdown: operator detail"));
  EXPECT_FALSE(isCleanDriveShutdownReason(formatDriveDownReason(DriveDownReason::Startup)));
  EXPECT_FALSE(isCleanDriveShutdownReason(formatDriveDownReason(DriveDownReason::StartupRecoveryDisabled)));
  EXPECT_FALSE(isCleanDriveShutdownReason(formatDriveDownReason(DriveDownReason::DriveCleanupFailed)));
  EXPECT_FALSE(isCleanDriveShutdownReason(formatDriveDownReason(DriveDownReason::SessionDidNotStopSafely)));
}
}  // namespace cta::common::dataStructures
