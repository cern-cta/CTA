/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include "DriveDownReason.hpp"

#include "common/log/Logger.hpp"

#include <gtest/gtest.h>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <tuple>

namespace cta::common::dataStructures {
TEST(DriveDownReasonTest, FormatsStableCodeLocalTimestampSeverityAndDetails) {
  const std::time_t recordedAt = 0;
  std::tm localTime {};
  ASSERT_NE(nullptr, ::localtime_r(&recordedAt, &localTime));
  std::ostringstream timestamp;
  timestamp << std::put_time(&localTime, "%FT%T");

  for (const auto& [reason, severity, prefix, message] : {
         std::tuple {DriveDownReason::Startup,            log::INFO, "[cta-taped:startup] ", " INFO Startup"              },
         std::tuple {DriveDownReason::DriveCleanupFailed,
                     log::ERR,
                     "[cta-taped:drive-cleanup-failed] ",                                    " ERROR Drive cleanup failed"}
  }) {
    const auto expected = prefix + timestamp.str() + message;
    EXPECT_EQ(severity, driveDownReasonSeverity(reason));
    EXPECT_EQ(expected, formatDriveDownReason(reason, {}, recordedAt));
    EXPECT_EQ(expected + ": device unavailable", formatDriveDownReason(reason, "device unavailable", recordedAt));
  }
}

TEST(DriveDownReasonTest, RejectsUnknownReasons) {
  const auto unknownReason = static_cast<DriveDownReason>(-1);
  EXPECT_THROW(driveDownReasonSeverity(unknownReason), std::invalid_argument);
  EXPECT_THROW(formatDriveDownReason(unknownReason), std::invalid_argument);
}

TEST(DriveDownReasonTest, ClassifiesIdentifiersIndependentlyOfDisplayTextAndTime) {
  using Kind = PersistedDriveReasonKind;
  EXPECT_EQ(Kind::Startup, classifyPersistedDriveReason("[cta-taped:startup] 2026-10-13T14:25:30Z New wording"));
  EXPECT_EQ(Kind::CleanShutdown,
            classifyPersistedDriveReason("[cta-taped:shutdown] 2024-02-29T23:59:59Z INFO Changed wording: SIGTERM"));
  EXPECT_EQ(Kind::CleanShutdown,
            classifyPersistedDriveReason(formatDriveDownReason(DriveDownReason::Shutdown, "SIGTERM", 0)));
  EXPECT_EQ(Kind::Other, classifyPersistedDriveReason(formatDriveDownReason(DriveDownReason::StartupRecoveryDisabled)));
  EXPECT_EQ(Kind::Other, classifyPersistedDriveReason(formatDriveDownReason(DriveDownReason::DriveCleanupFailed)));
}

TEST(DriveDownReasonTest, ClassifiesPrefixWithoutRequiringDisplayText) {
  EXPECT_EQ(PersistedDriveReasonKind::Startup, classifyPersistedDriveReason("[cta-taped:startup]"));
  EXPECT_EQ(PersistedDriveReasonKind::CleanShutdown, classifyPersistedDriveReason("[cta-taped:shutdown]"));
  EXPECT_EQ(PersistedDriveReasonKind::CleanShutdown,
            classifyPersistedDriveReason("[cta-taped:shutdown] arbitrary display text"));
}

TEST(DriveDownReasonTest, UnrecognizedPrefixesRemainOperatorControlled) {
  for (const auto reason : {"",
                            "Operator requested maintenance",
                            "[cta-taped] INFO Startup",
                            "[cta-taped] INFO Shutdown",
                            "[cta-taped:future-code] INFO Shutdown",
                            "[cta-taped:v1:shutdown] INFO Shutdown",
                            "[cta-taped:shutdown-extra] INFO Shutdown",
                            "[cta-taped:shutdown",
                            "prefix [cta-taped:shutdown]"}) {
    SCOPED_TRACE(reason);
    EXPECT_EQ(PersistedDriveReasonKind::Other, classifyPersistedDriveReason(reason));
  }
}
}  // namespace cta::common::dataStructures
