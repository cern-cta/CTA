/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include "DriveDownReason.hpp"

#include "common/log/Logger.hpp"

#include <gtest/gtest.h>
#include <stdexcept>
#include <tuple>

namespace cta::common::dataStructures {
TEST(DriveDownReasonTest, FormatsMessageAndDetailsWithSeparateLoggingSeverity) {
  for (const auto& [reason, severity, message] : {
         std::tuple {DriveDownReason::Startup,                  log::INFO, "Daemon started"             },
         std::tuple {DriveDownReason::StartupRecoveryDisabled,
                     log::INFO,
                     "Daemon started: desired Up detected, but startup recovery is disabled"            },
         std::tuple {DriveDownReason::Shutdown,                 log::INFO, "Daemon shutting down"       },
         std::tuple {DriveDownReason::TapeDetected,             log::ERR,  "Tape detected in drive"     },
         std::tuple {DriveDownReason::DriveProbeFailed,         log::ERR,  "Drive probe failed"         },
         std::tuple {DriveDownReason::SessionDriveAccessFailed, log::ERR,  "Drive access failed"        },
         std::tuple {DriveDownReason::DriveCleanupFailed,       log::ERR,  "Drive cleanup failed"       },
         std::tuple {DriveDownReason::SessionLeftDriveUnusable, log::ERR,  "Drive reuse unsafe"         },
         std::tuple {DriveDownReason::UnexpectedFailure,        log::ERR,  "Unexpected failure"         },
         std::tuple {DriveDownReason::SessionDidNotStopSafely,  log::ERR,  "Session did not stop safely"}
  }) {
    const std::string expected = std::string("[cta-taped] ") + message;
    EXPECT_EQ(severity, driveDownReasonSeverity(reason));
    EXPECT_EQ(expected, formatDriveDownReason(reason));
    EXPECT_EQ(expected + ": device unavailable", formatDriveDownReason(reason, "device unavailable"));
  }
}

TEST(DriveDownReasonTest, RejectsUnknownReasons) {
  const auto unknownReason = static_cast<DriveDownReason>(-1);
  EXPECT_THROW(driveDownReasonSeverity(unknownReason), std::invalid_argument);
  EXPECT_THROW(formatDriveDownReason(unknownReason), std::invalid_argument);
}

TEST(DriveDownReasonTest, ClassifiesStartupAndShutdownMessages) {
  using Kind = PersistedDriveReasonKind;
  EXPECT_EQ(Kind::Startup, classifyPersistedDriveReason(formatDriveDownReason(DriveDownReason::Startup)));
  EXPECT_EQ(Kind::CleanShutdown, classifyPersistedDriveReason(formatDriveDownReason(DriveDownReason::Shutdown)));
  EXPECT_EQ(Kind::CleanShutdown,
            classifyPersistedDriveReason(formatDriveDownReason(DriveDownReason::Shutdown, "SIGTERM")));
  EXPECT_EQ(Kind::Other, classifyPersistedDriveReason(formatDriveDownReason(DriveDownReason::StartupRecoveryDisabled)));
  EXPECT_EQ(Kind::Other, classifyPersistedDriveReason(formatDriveDownReason(DriveDownReason::DriveCleanupFailed)));
}

TEST(DriveDownReasonTest, SpecificAndUnrecognizedMessagesRemainOperatorControlled) {
  // Similar text and old persisted formats must not accidentally permit automatic startup.
  for (const auto reason : {"",
                            "Operator requested maintenance",
                            "[cta-taped] Daemon started: inspect drive",
                            "[cta-taped] Daemon shutting down unexpectedly",
                            "[cta-taped] Daemon shutting down:missing separator",
                            "Daemon shutting down",
                            "prefix [cta-taped] Daemon shutting down",
                            "[cta-taped] INFO Shutdown",
                            "[cta-taped:startup]",
                            "[cta-taped:shutdown] 2026-10-13T14:25:30 INFO Shutdown"}) {
    SCOPED_TRACE(reason);
    EXPECT_EQ(PersistedDriveReasonKind::Other, classifyPersistedDriveReason(reason));
  }
}
}  // namespace cta::common::dataStructures
