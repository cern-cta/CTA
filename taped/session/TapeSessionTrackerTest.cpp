/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapeSessionTracker.hpp"

#include <gtest/gtest.h>
#include <thread>

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

TEST(TapeSessionTrackerTest, SetsDeliveryTimeWithoutReplacingAccumulatedDiskStats) {
  TapeSessionTracker tracker;
  tracker.addDiskStats({.waitReportingTime = 3});

  tracker.setDiskDeliveryTime(8);

  EXPECT_EQ(8, tracker.diskStats().deliveryTime);
  EXPECT_EQ(3, tracker.diskStats().waitReportingTime);
}

TEST(TapeSessionTrackerTest, TracksActiveDiskFilesByThread) {
  TapeSessionTracker tracker;

  tracker.notifyDiskFileOpened(1, 1234, "file:///one");
  tracker.notifyDiskFileOpened(2, 5678, "file:///two");

  const auto files = tracker.activeDiskFiles();
  ASSERT_EQ(2, files.size());
  EXPECT_EQ(1234, files.at(1).fileId);
  EXPECT_EQ("file:///one", files.at(1).path);
  EXPECT_NE(std::chrono::steady_clock::time_point {}, files.at(1).openedAt);
  EXPECT_EQ(5678, files.at(2).fileId);

  tracker.notifyDiskFileClosed(1);
  EXPECT_FALSE(tracker.activeDiskFiles().contains(1));
  EXPECT_TRUE(tracker.activeDiskFiles().contains(2));
}

TEST(TapeSessionTrackerTest, ConcurrentDiskThreadsRetainIndependentEntries) {
  TapeSessionTracker tracker;

  std::thread first([&tracker] { tracker.notifyDiskFileOpened(1, 1234, "file:///one"); });
  std::thread second([&tracker] { tracker.notifyDiskFileOpened(2, 5678, "file:///two"); });
  first.join();
  second.join();

  const auto files = tracker.activeDiskFiles();
  ASSERT_EQ(2, files.size());
  EXPECT_EQ(1234, files.at(1).fileId);
  EXPECT_EQ(5678, files.at(2).fileId);
}

TEST(TapeSessionTrackerTest, TracksBlockMovement) {
  TapeSessionTracker tracker;
  const auto initialMovement = tracker.lastBlockMovement();

  tracker.notifyBeginNewJob(1234, 42);
  tracker.notifyBlockMovement(10);
  const auto firstMovement = tracker.lastBlockMovement();
  tracker.notifyBlockMovement(15);

  const auto progress = tracker.progress();
  EXPECT_EQ(25, tracker.bytesMoved());
  EXPECT_GT(firstMovement, initialMovement);
  EXPECT_GE(tracker.lastBlockMovement(), firstMovement);
  EXPECT_EQ(1234, progress.fileId);
  EXPECT_EQ(42, progress.fSeq);
  EXPECT_TRUE(progress.fileBeingMoved);
  EXPECT_NE(std::chrono::steady_clock::time_point {}, progress.fileStartTime);
  EXPECT_EQ(25, progress.bytesMoved);
  EXPECT_EQ(tracker.lastBlockMovement(), progress.lastBlockMovement);

  tracker.fileFinished();
  const auto finishedProgress = tracker.progress();
  EXPECT_EQ(0, finishedProgress.fileId);
  EXPECT_EQ(0, finishedProgress.fSeq);
  EXPECT_FALSE(finishedProgress.fileBeingMoved);
  EXPECT_EQ(std::chrono::steady_clock::time_point {}, finishedProgress.fileStartTime);
}

TEST(TapeSessionTrackerTest, TracksSessionElapsedTimeFromScheduling) {
  TapeSessionTracker tracker;

  EXPECT_EQ(std::chrono::steady_clock::time_point {}, tracker.sessionStartTime());
  EXPECT_EQ(std::chrono::steady_clock::duration {}, tracker.sessionElapsedTime());

  tracker.reportState(cta::tape::session::SessionState::Scheduling, cta::tape::session::SessionType::Undetermined);
  const auto startTime = tracker.sessionStartTime();

  EXPECT_NE(std::chrono::steady_clock::time_point {}, startTime);
  EXPECT_GE(tracker.sessionElapsedTime(), std::chrono::steady_clock::duration {});

  tracker.reportState(cta::tape::session::SessionState::Scheduling, cta::tape::session::SessionType::Undetermined);
  EXPECT_EQ(startTime, tracker.sessionStartTime());
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
  tracker.notifyDiskFileOpened(1, 1234, "file:///one");

  tracker.reportState(cta::tape::session::SessionState::Scheduling, cta::tape::session::SessionType::Undetermined);

  EXPECT_EQ(0, tracker.tapeStats().filesCount);
  EXPECT_EQ(0, tracker.diskStats().deliveryTime);
  EXPECT_FALSE(tracker.errorHappened());
  EXPECT_TRUE(tracker.tapeAlertStats().empty());
  EXPECT_EQ(0, tracker.bytesMoved());
  EXPECT_EQ(std::chrono::steady_clock::time_point {}, tracker.lastBlockMovement());
  EXPECT_TRUE(tracker.activeDiskFiles().empty());
  const auto sessionStartTime = tracker.sessionStartTime();
  EXPECT_NE(std::chrono::steady_clock::time_point {}, sessionStartTime);

  tracker.updateTapeStats(tapeStats);
  tracker.incrementError(TapeSessionError::DiskRead);
  tracker.incrementTapeAlert(0x01);
  tracker.notifyBlockMovement(100);
  tracker.reportState(cta::tape::session::SessionState::Scheduling, cta::tape::session::SessionType::Undetermined);

  EXPECT_EQ(2, tracker.tapeStats().filesCount);
  EXPECT_TRUE(tracker.errorHappened());
  EXPECT_EQ(1, tracker.tapeAlertStats().at(0x01));
  EXPECT_EQ(100, tracker.bytesMoved());
  EXPECT_EQ(sessionStartTime, tracker.sessionStartTime());
}

}  // namespace cta::tape::daemon
