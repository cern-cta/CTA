/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include "DriveDownReason.hpp"

#include "common/log/Logger.hpp"

#include <gtest/gtest.h>
#include <stdexcept>

namespace cta::common::dataStructures {
TEST(DriveDownReasonTest, FormatsSeverityAndDetails) {
  const struct {
    DriveDownReason reason;
    int severity;
    const char* formatted;
  } cases[] = {
    {DriveDownReason::Startup,                  log::INFO, "[cta-taped] INFO Startup"                     },
    {DriveDownReason::Shutdown,                 log::INFO, "[cta-taped] INFO Shutdown"                    },
    {DriveDownReason::TapeDetected,             log::ERR,  "[cta-taped] ERROR Tape detected in drive"     },
    {DriveDownReason::DriveProbeFailed,         log::ERR,  "[cta-taped] ERROR Drive probe failed"         },
    {DriveDownReason::SessionDriveAccessFailed, log::ERR,  "[cta-taped] ERROR Session drive access failed"},
    {DriveDownReason::DriveCleanupFailed,       log::ERR,  "[cta-taped] ERROR Drive cleanup failed"       },
    {DriveDownReason::SessionLeftDriveUnusable, log::ERR,  "[cta-taped] ERROR Session left drive unusable"}
  };

  for (const auto& testCase : cases) {
    SCOPED_TRACE(testCase.formatted);
    EXPECT_EQ(testCase.severity, driveDownReasonSeverity(testCase.reason));
    EXPECT_EQ(testCase.formatted, formatDriveDownReason(testCase.reason));
    EXPECT_EQ(std::string(testCase.formatted) + ": device unavailable",
              formatDriveDownReason(testCase.reason, "device unavailable"));
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
  EXPECT_FALSE(isCleanDriveShutdownReason(formatDriveDownReason(DriveDownReason::DriveCleanupFailed)));
}
}  // namespace cta::common::dataStructures
