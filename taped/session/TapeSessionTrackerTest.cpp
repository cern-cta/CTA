/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapeSessionTracker.hpp"

#include <atomic>
#include <gtest/gtest.h>
#include <thread>

namespace cta::tape::daemon {

TEST(TapeSessionTrackerTest, UpdatesTransferAndCleanupStatsIndependently) {
  TapeSessionTracker tracker;

  TapeTransferStats tapeStats;
  tapeStats.dataVolume = 10;
  tracker.setTotalTime(5);
  tracker.updateTapeTransferStats(tapeStats);

  TapeTransferStats additionalTapeStats;
  additionalTapeStats.dataVolume = 15;
  tracker.addTapeTransferStats(additionalTapeStats);

  DiskTransferStats diskStats;
  diskStats.deliveryTime = 8;
  diskStats.waitReportingTime = 2;
  tracker.updateDiskTransferStats(diskStats);

  DiskTransferStats additionalDiskStats;
  additionalDiskStats.deliveryTime = 50;
  additionalDiskStats.waitReportingTime = 3;
  tracker.addDiskTransferStats(additionalDiskStats);

  EXPECT_EQ(25, tracker.stats().tape.dataVolume);
  EXPECT_EQ(5, tracker.stats().totalTime);
  EXPECT_EQ(8, tracker.stats().disk.deliveryTime);
  EXPECT_EQ(5, tracker.stats().disk.waitReportingTime);

  tracker.updateTapeCleanupStats({.cleanupTime = 4, .lbpResetTime = 1});
  tracker.addTapeCleanupStats({.unloadTime = 2, .cleanupTime = 3, .lbpResetTime = 2});
  tracker.updateTapeTransferStats({.dataVolume = 30});
  const auto stats = tracker.stats();
  EXPECT_EQ(30, stats.tape.dataVolume);
  EXPECT_EQ(8, stats.disk.deliveryTime);
  EXPECT_EQ(5, stats.disk.waitReportingTime);
  EXPECT_EQ(7, stats.cleanup.cleanupTime);
  EXPECT_EQ(3, stats.cleanup.lbpResetTime);
  EXPECT_EQ(2, stats.cleanup.unloadTime);
  EXPECT_EQ(5, stats.totalTime);
}

TEST(TapeSessionTrackerTest, SetupStatsAccumulateAndSurviveTransferReplacement) {
  TapeSessionTracker tracker;
  tracker.updateTapeSetupStats(
    {.mountTime = 1, .initialMountTime = 2, .tapeLoadTime = 3, .encryptionControlTime = 4, .positionTime = 5});
  tracker.addTapeSetupStats(
    {.mountTime = 6, .initialMountTime = 7, .tapeLoadTime = 8, .encryptionControlTime = 9, .positionTime = 10});
  tracker.addTapeCleanupStats({.cleanupTime = 11});
  tracker.addDiskTransferStats({.waitReportingTime = 12});
  tracker.updateTapeTransferStats({.dataVolume = 13});
  const auto snapshot = tracker.stats();
  EXPECT_EQ(7, snapshot.setup.mountTime);
  EXPECT_EQ(9, snapshot.setup.initialMountTime);
  EXPECT_EQ(11, snapshot.setup.tapeLoadTime);
  EXPECT_EQ(13, snapshot.setup.encryptionControlTime);
  EXPECT_EQ(15, snapshot.setup.positionTime);
  EXPECT_EQ(11, snapshot.cleanup.cleanupTime);
  EXPECT_EQ(12, snapshot.disk.waitReportingTime);
  EXPECT_EQ(13, snapshot.tape.dataVolume);

  tracker.updateTapeSetupStats({.tapeLoadTime = 14});
  EXPECT_EQ(0, tracker.stats().setup.mountTime);
  EXPECT_EQ(14, tracker.stats().setup.tapeLoadTime);
  EXPECT_EQ(13, tracker.stats().tape.dataVolume);
  EXPECT_EQ(11, snapshot.setup.tapeLoadTime);
}

TEST(TapeSessionTrackerTest, ConcurrentComponentUpdatesProduceConsistentSnapshots) {
  TapeSessionTracker tracker;
  std::atomic<unsigned> finished = 0;
  constexpr unsigned iterations = 1000;

  std::thread tape([&] {
    for (unsigned i = 0; i < iterations; ++i) {
      tracker.addTapeTransferStats({.dataVolume = 1, .filesCount = 1});
    }
    ++finished;
  });
  std::thread disk([&] {
    for (unsigned i = 0; i < iterations; ++i) {
      tracker.addDiskTransferStats({.waitReportingTime = 1});
    }
    ++finished;
  });
  std::thread cleanup([&] {
    for (unsigned i = 0; i < iterations; ++i) {
      tracker.addTapeCleanupStats({.unloadTime = 2, .cleanupTime = 1});
    }
    ++finished;
  });

  while (finished != 3) {
    const auto stats = tracker.stats();
    EXPECT_EQ(stats.tape.dataVolume, stats.tape.filesCount);
    EXPECT_EQ(2 * stats.cleanup.cleanupTime, stats.cleanup.unloadTime);
  }
  tape.join();
  disk.join();
  cleanup.join();

  const auto snapshot = tracker.stats();
  EXPECT_EQ(iterations, snapshot.tape.dataVolume);
  EXPECT_EQ(iterations, snapshot.disk.waitReportingTime);
  EXPECT_EQ(iterations, snapshot.cleanup.cleanupTime);
  tracker.updateTapeTransferStats({});
  EXPECT_EQ(iterations, snapshot.tape.dataVolume);
  EXPECT_EQ(iterations, tracker.stats().cleanup.cleanupTime);
}

TEST(TapeSessionTrackerTest, SetsDeliveryTimeWithoutReplacingAccumulatedDiskStats) {
  TapeSessionTracker tracker;
  tracker.addDiskTransferStats({.waitReportingTime = 3});

  tracker.setDiskDeliveryTime(8);

  EXPECT_EQ(8, tracker.stats().disk.deliveryTime);
  EXPECT_EQ(3, tracker.stats().disk.waitReportingTime);
}

TEST(TapeSessionTrackerTest, StoresTypedSessionOutcomeAndMountAttemptState) {
  TapeSessionTracker tracker;

  tracker.setOutcome(TapeSessionOutcome::Failure);
  tracker.setMountAttempted(false);

  EXPECT_EQ(TapeSessionOutcome::Failure, tracker.outcome());
  EXPECT_FALSE(tracker.mountAttempted());
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

TEST(TapeSessionTrackerTest, TracksSessionElapsedTimeFromBeginTapeSession) {
  TapeSessionTracker tracker;

  EXPECT_FALSE(tracker.state().has_value());
  EXPECT_EQ(std::chrono::steady_clock::time_point {}, tracker.sessionStartTime());
  EXPECT_EQ(std::chrono::steady_clock::duration {}, tracker.sessionElapsedTime());

  tracker.beginTapeSession();
  const auto startTime = tracker.sessionStartTime();

  EXPECT_NE(std::chrono::steady_clock::time_point {}, startTime);
  EXPECT_GE(tracker.sessionElapsedTime(), std::chrono::steady_clock::duration {});

  tracker.reportState(cta::tape::session::TapeSessionState::Preparing);
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

TEST(TapeSessionTrackerTest, BeginningTransferResetsDataAndTransitionsPreserveIt) {
  TapeSessionTracker tracker;
  TapeTransferStats tapeStats;
  tapeStats.filesCount = 2;
  DiskTransferStats diskStats;
  diskStats.deliveryTime = 3;

  tracker.updateTapeTransferStats(tapeStats);
  tracker.updateDiskTransferStats(diskStats);
  tracker.addTapeSetupStats({.mountTime = 6, .tapeLoadTime = 7});
  tracker.addTapeCleanupStats({.cleanupTime = 4});
  tracker.setTotalTime(5);
  tracker.incrementError(TapeSessionError::DiskRead);
  tracker.incrementTapeAlert(0x01);
  tracker.notifyBlockMovement(100);
  tracker.notifyDiskFileOpened(1, 1234, "file:///one");
  tracker.setOutcome(TapeSessionOutcome::Failure);
  tracker.setMountAttempted(false);

  tracker.beginTapeSession();

  EXPECT_EQ(0, tracker.stats().setup.mountTime);
  EXPECT_EQ(0, tracker.stats().setup.tapeLoadTime);
  EXPECT_EQ(0, tracker.stats().tape.filesCount);
  EXPECT_EQ(0, tracker.stats().disk.deliveryTime);
  EXPECT_EQ(0, tracker.stats().cleanup.cleanupTime);
  EXPECT_EQ(0, tracker.stats().totalTime);
  EXPECT_FALSE(tracker.errorHappened());
  EXPECT_TRUE(tracker.tapeAlertStats().empty());
  EXPECT_EQ(0, tracker.bytesMoved());
  EXPECT_EQ(std::chrono::steady_clock::time_point {}, tracker.lastBlockMovement());
  EXPECT_TRUE(tracker.activeDiskFiles().empty());
  EXPECT_EQ(TapeSessionOutcome::Automatic, tracker.outcome());
  EXPECT_TRUE(tracker.mountAttempted());
  const auto sessionStartTime = tracker.sessionStartTime();
  EXPECT_NE(std::chrono::steady_clock::time_point {}, sessionStartTime);

  tracker.updateTapeTransferStats(tapeStats);
  tracker.addTapeSetupStats({.mountTime = 6, .tapeLoadTime = 7});
  tracker.addTapeCleanupStats({.cleanupTime = 4});
  tracker.setTotalTime(5);
  tracker.incrementError(TapeSessionError::DiskRead);
  tracker.incrementTapeAlert(0x01);
  tracker.notifyBlockMovement(100);
  tracker.setType(cta::tape::session::SessionType::Retrieve);

  // Tape operation phases must preserve session counters, timing, and type.
  using cta::tape::session::TapeSessionState;
  for (const auto state : {TapeSessionState::Mounting,
                           TapeSessionState::Loading,
                           TapeSessionState::Transferring,
                           TapeSessionState::Unloading,
                           TapeSessionState::Unmounting,
                           TapeSessionState::Finalizing}) {
    tracker.reportState(state);
    EXPECT_EQ(state, tracker.state());

    EXPECT_EQ(6, tracker.stats().setup.mountTime);
    EXPECT_EQ(7, tracker.stats().setup.tapeLoadTime);
    EXPECT_EQ(2, tracker.stats().tape.filesCount);
    EXPECT_EQ(4, tracker.stats().cleanup.cleanupTime);
    EXPECT_EQ(5, tracker.stats().totalTime);
    EXPECT_TRUE(tracker.errorHappened());
    EXPECT_EQ(1, tracker.tapeAlertStats().at(0x01));
    EXPECT_EQ(100, tracker.bytesMoved());
    EXPECT_EQ(sessionStartTime, tracker.sessionStartTime());
    EXPECT_EQ(cta::tape::session::SessionType::Retrieve, tracker.type());
  }
}

TEST(TapeSessionTrackerTest, BeginningAgainResetsPreparationFailuresAndCompletionFlags) {
  using cta::tape::session::TapeSessionState;
  TapeSessionTracker tracker;
  tracker.beginTapeSession();
  tracker.incrementError(TapeSessionError::DiskRead);
  tracker.setOutcome(TapeSessionOutcome::Failure);
  tracker.notifyBeginNewJob(12, 34);
  tracker.notifyDiskDone();
  EXPECT_EQ(TapeSessionState::Preparing, tracker.state());

  tracker.beginTapeSession();
  EXPECT_EQ(TapeSessionState::Preparing, tracker.state());
  EXPECT_FALSE(tracker.errorHappened());
  EXPECT_EQ(TapeSessionOutcome::Automatic, tracker.outcome());
  EXPECT_FALSE(tracker.progress().fileBeingMoved);
  EXPECT_EQ(0, tracker.progress().fileId);
  EXPECT_EQ(0, tracker.progress().fSeq);
  EXPECT_EQ(cta::tape::session::SessionType::Undetermined, tracker.type());

  tracker.setType(cta::tape::session::SessionType::Retrieve);
  tracker.notifyTapeDone();
  EXPECT_EQ(TapeSessionState::DrainingToDisk, tracker.state());
  tracker.beginTapeSession();
  tracker.setType(cta::tape::session::SessionType::Retrieve);
  tracker.notifyDiskDone();
  EXPECT_EQ(TapeSessionState::Preparing, tracker.state());
}

TEST(TapeSessionTrackerTest, RetrievalCompletionTracksEitherWorkerOrderAndFailures) {
  using cta::tape::session::TapeSessionState;
  for (bool diskFirst : {false, true}) {
    for (bool failed : {false, true}) {
      TapeSessionTracker tracker;
      tracker.beginTapeSession();
      tracker.setType(cta::tape::session::SessionType::Retrieve);
      tracker.reportState(TapeSessionState::Transferring);
      if (diskFirst) {
        tracker.notifyDiskDone();
        EXPECT_EQ(TapeSessionState::Transferring, tracker.state());
      }
      tracker.reportState(TapeSessionState::Finalizing);
      if (failed) {
        tracker.incrementError(TapeSessionError::TapeUnload);
        tracker.setOutcome(TapeSessionOutcome::Failure);
      }
      tracker.notifyTapeDone();
      EXPECT_EQ(diskFirst ? TapeSessionState::Finalizing : TapeSessionState::DrainingToDisk, tracker.state());
      tracker.notifyDiskDone();
      EXPECT_EQ(TapeSessionState::Finalizing, tracker.state());
      tracker.reportState(TapeSessionState::Finished);
      tracker.notifyTapeDone();
      tracker.notifyDiskDone();
      EXPECT_EQ(TapeSessionState::Finished, tracker.state());
      EXPECT_EQ(failed ? TapeSessionOutcome::Failure : TapeSessionOutcome::Automatic, tracker.outcome());
    }
  }
}

TEST(TapeSessionTrackerTest, ExplicitFailureSurvivesSuccessAndAutomaticAssignments) {
  TapeSessionTracker tracker;
  tracker.beginTapeSession();
  tracker.setOutcome(TapeSessionOutcome::Failure);
  tracker.setOutcome(TapeSessionOutcome::Success);
  tracker.setOutcome(TapeSessionOutcome::Automatic);
  EXPECT_EQ(TapeSessionOutcome::Failure, tracker.outcome());

  tracker.beginTapeSession();
  tracker.setOutcome(TapeSessionOutcome::Success);
  tracker.incrementError(TapeSessionError::Reporting);
  tracker.setOutcome(TapeSessionOutcome::Success);
  EXPECT_EQ(TapeSessionOutcome::Failure, tracker.outcome());
}

}  // namespace cta::tape::daemon
