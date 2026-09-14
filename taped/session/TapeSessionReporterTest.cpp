/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapeSessionReporter.hpp"

#include "common/log/StringLogger.hpp"

#include <chrono>
#include <gtest/gtest.h>
#include <thread>

namespace cta::tape::daemon {

using namespace std::chrono_literals;

TEST(TapeSessionReporterTest, ReportsTrackerContentsOnDemand) {
  cta::log::StringLogger log("dummy", "TapeSessionReporterTest", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  TapeSessionTracker tracker;
  TapeSessionReporter reporter(tracker, lc, 1s, 1s);

  tracker.notifyBlockMovement(25);
  tracker.incrementError(TapeSessionError::DiskRead);

  reporter.reportNow();

  EXPECT_NE(std::string::npos, log.getLog().find("Tape session statistics"));
  EXPECT_NE(std::string::npos, log.getLog().find("Error_diskRead"));
}

TEST(TapeSessionReporterTest, PeriodicallyReportsAndFlushesOnShutdown) {
  cta::log::StringLogger log("dummy", "TapeSessionReporterTest", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  TapeSessionTracker tracker;
  TapeSessionReporter reporter(tracker, lc, 5ms, 1s);

  reporter.startThreads();
  std::this_thread::sleep_for(20ms);
  reporter.finish();
  reporter.waitThreads();

  EXPECT_NE(std::string::npos, log.getLog().find("Tape session statistics"));
  EXPECT_NE(std::string::npos, log.getLog().find("Tape session finished"));
  EXPECT_NE(std::string::npos, log.getLog().find("tape_session_finished"));
}

TEST(TapeSessionReporterTest, AddsSessionParametersToFinishedEvent) {
  cta::log::StringLogger log("dummy", "TapeSessionReporterTest", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  TapeSessionTracker tracker;
  TapeSessionReporter reporter(tracker, lc, 1s, 1s);

  reporter.addParameters({
    {"tapeDrive",      "drive0"},
    {"mountAttempted", 1       },
    {"status",         "error" }
  });
  reporter.addParameter({"tapeVid", "V12345"});
  reporter.deleteParameter("tapeVid");
  reporter.startThreads();
  reporter.finish();
  reporter.waitThreads();

  EXPECT_NE(std::string::npos, log.getLog().find("tapeDrive"));
  EXPECT_NE(std::string::npos, log.getLog().find("drive0"));
  EXPECT_NE(std::string::npos, log.getLog().find("\"status\":\"error\""));
  EXPECT_EQ(std::string::npos, log.getLog().find("V12345"));
}

TEST(TapeSessionReporterTest, ReportsAStuckFile) {
  cta::log::StringLogger log("dummy", "TapeSessionReporterTest", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  TapeSessionTracker tracker;
  TapeSessionReporter reporter(tracker, lc, 5ms, 5ms);

  tracker.notifyBeginNewJob(1234, 42);
  reporter.startThreads();
  std::this_thread::sleep_for(20ms);
  reporter.finish();
  reporter.waitThreads();

  EXPECT_NE(std::string::npos, log.getLog().find("No tape block movement for too long"));
}

}  // namespace cta::tape::daemon
