/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapeSessionTracker.hpp"

#include <gtest/gtest.h>

namespace cta::tape::daemon {

TEST(TapeSessionTrackerTest, UpdatesTapeAndDiskStatsIndependently) {
  TapeSessionTracker tracker;

  TapeSideStats tapeStats;
  tapeStats.dataVolume = 10;
  tapeStats.totalTime = 5;
  tracker.updateTapeStats(tapeStats);

  TapeSideStats additionalTapeStats;
  additionalTapeStats.dataVolume = 15;
  additionalTapeStats.totalTime = 20;
  tracker.addTapeStats(additionalTapeStats);

  DiskSideStats diskStats;
  diskStats.deliveryTime = 8;
  diskStats.waitReportingTime = 2;
  tracker.updateDiskStats(diskStats);

  DiskSideStats additionalDiskStats;
  additionalDiskStats.deliveryTime = 50;
  additionalDiskStats.waitReportingTime = 3;
  tracker.addDiskStats(additionalDiskStats);

  EXPECT_EQ(25, tracker.tapeStats().dataVolume);
  EXPECT_EQ(5, tracker.tapeStats().totalTime);
  EXPECT_EQ(8, tracker.diskStats().deliveryTime);
  EXPECT_EQ(5, tracker.diskStats().waitReportingTime);
}

TEST(TapeSessionTrackerTest, TracksBlockMovement) {
  TapeSessionTracker tracker;
  const auto initialMovement = tracker.lastBlockMovement();

  tracker.notifyBlockMovement(10);
  const auto firstMovement = tracker.lastBlockMovement();
  tracker.notifyBlockMovement(15);

  EXPECT_EQ(25, tracker.bytesMoved());
  EXPECT_GT(firstMovement, initialMovement);
  EXPECT_GE(tracker.lastBlockMovement(), firstMovement);
}

TEST(TapeSessionTrackerTest, SetsAndClearsErrorCounts) {
  TapeSessionTracker tracker;

  tracker.incrementError(TapeSessionError::Reporting);
  tracker.setErrorCount(TapeSessionError::Reporting, 4);

  ASSERT_TRUE(tracker.errorStats().contains(TapeSessionError::Reporting));
  EXPECT_EQ(4, tracker.errorStats().at(TapeSessionError::Reporting));

  tracker.setErrorCount(TapeSessionError::Reporting, 0);

  EXPECT_FALSE(tracker.errorHappened());
  EXPECT_FALSE(tracker.errorStats().contains(TapeSessionError::Reporting));
}

TEST(TapeSessionTrackerTest, CountsTapeAlertsByCode) {
  TapeSessionTracker tracker;

  tracker.incrementTapeAlert(0x01);
  tracker.incrementTapeAlert(0x01);
  tracker.incrementTapeAlert(0x32);

  const auto tapeAlerts = tracker.tapeAlertStats();
  EXPECT_EQ(2, tapeAlerts.at(0x01));
  EXPECT_EQ(1, tapeAlerts.at(0x32));
  EXPECT_TRUE(tracker.errorHappened());
}

TEST(TapeSessionTrackerTest, EnteringSchedulingResetsSessionDataOnce) {
  TapeSessionTracker tracker;
  TapeSideStats tapeStats;
  tapeStats.filesCount = 2;
  DiskSideStats diskStats;
  diskStats.deliveryTime = 3;

  tracker.updateTapeStats(tapeStats);
  tracker.updateDiskStats(diskStats);
  tracker.incrementError(TapeSessionError::DiskRead);
  tracker.incrementTapeAlert(0x01);
  tracker.notifyBlockMovement(100);

  tracker.reportState(cta::tape::session::SessionState::Scheduling, cta::tape::session::SessionType::Undetermined);

  EXPECT_EQ(0, tracker.tapeStats().filesCount);
  EXPECT_EQ(0, tracker.diskStats().deliveryTime);
  EXPECT_FALSE(tracker.errorHappened());
  EXPECT_TRUE(tracker.tapeAlertStats().empty());
  EXPECT_EQ(0, tracker.bytesMoved());
  EXPECT_EQ(std::chrono::steady_clock::time_point {}, tracker.lastBlockMovement());

  tracker.updateTapeStats(tapeStats);
  tracker.incrementError(TapeSessionError::DiskRead);
  tracker.incrementTapeAlert(0x01);
  tracker.notifyBlockMovement(100);
  tracker.reportState(cta::tape::session::SessionState::Scheduling, cta::tape::session::SessionType::Undetermined);

  EXPECT_EQ(2, tracker.tapeStats().filesCount);
  EXPECT_TRUE(tracker.errorHappened());
  EXPECT_EQ(1, tracker.tapeAlertStats().at(0x01));
  EXPECT_EQ(100, tracker.bytesMoved());
}

}  // namespace cta::tape::daemon
