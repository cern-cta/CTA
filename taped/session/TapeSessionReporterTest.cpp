/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapeSessionReporter.hpp"

#include "common/log/StringLogger.hpp"
#include "scheduler/TapeMountDummy.hpp"

#include <chrono>
#include <gtest/gtest.h>
#include <thread>

namespace cta::tape::daemon {

using namespace std::chrono_literals;

class ReportingTapeMount : public cta::TapeMountDummy {
public:
  std::string getMountTransactionId() const override { return "12345"; }

  cta::common::dataStructures::MountType getMountType() const override {
    return cta::common::dataStructures::MountType::Retrieve;
  }

  std::string getVid() const override { return "V12345"; }

  std::string getVo() const override { return "vo"; }

  std::string getMediaType() const override { return "LTO"; }

  std::string getVendor() const override { return "vendor"; }

  std::string getPoolName() const override { return "pool"; }

  uint64_t getCapacityInBytes() const override { return 1000; }
};

TEST(TapeSessionReporterTest, ReportsTrackerContentsOnDemand) {
  cta::log::StringLogger log("dummy", "TapeSessionReporterTest", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  TapeSessionTracker tracker;
  ReportingTapeMount mount;
  TapeSessionReporter reporter(tracker, mount, lc, 1s, 1s);

  tracker.notifyBlockMovement(25);
  tracker.incrementError(TapeSessionError::DiskRead);

  reporter.reportNow();

  EXPECT_NE(std::string::npos, log.getLog().find("Tape session statistics"));
  EXPECT_NE(std::string::npos, log.getLog().find("Error_diskRead"));
  EXPECT_NE(std::string::npos, log.getLog().find("\"status\":\"failure\""));
}

TEST(TapeSessionReporterTest, PeriodicallyReportsAndFlushesOnShutdown) {
  cta::log::StringLogger log("dummy", "TapeSessionReporterTest", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  TapeSessionTracker tracker;
  ReportingTapeMount mount;
  TapeSessionReporter reporter(tracker, mount, lc, 5ms, 1s);

  reporter.startThreads();
  std::this_thread::sleep_for(20ms);
  reporter.finish();
  reporter.waitThreads();

  EXPECT_NE(std::string::npos, log.getLog().find("Tape session statistics"));
  EXPECT_NE(std::string::npos, log.getLog().find("Tape session finished"));
  EXPECT_NE(std::string::npos, log.getLog().find("tape_session_finished"));
}

TEST(TapeSessionReporterTest, DerivesMountMetadataAndUsesTypedOutcome) {
  cta::log::StringLogger log("dummy", "TapeSessionReporterTest", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  TapeSessionTracker tracker;
  ReportingTapeMount mount;
  TapeSessionReporter reporter(tracker, mount, lc, 1s, 1s);

  tracker.setOutcome(TapeSessionOutcome::Failure);
  tracker.setMountAttempted(false);
  reporter.startThreads();
  reporter.finish();
  reporter.waitThreads();

  EXPECT_NE(std::string::npos, log.getLog().find("\"status\":\"failure\""));
  EXPECT_NE(std::string::npos, log.getLog().find("\"mountAttempted\":0"));
  EXPECT_NE(std::string::npos, log.getLog().find("V12345"));
  EXPECT_NE(std::string::npos, log.getLog().find("vendor"));
  EXPECT_NE(std::string::npos, log.getLog().find("pool"));
}

TEST(TapeSessionReporterTest, ReportsOnlyActiveDiskFilesWithLegacyParameterNames) {
  cta::log::StringLogger log("dummy", "TapeSessionReporterTest", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  TapeSessionTracker tracker;
  ReportingTapeMount mount;
  TapeSessionReporter reporter(tracker, mount, lc, 1s, 1s);

  tracker.notifyDiskFileOpened(1, 1234, "file:///closed");
  tracker.notifyDiskFileOpened(2, 5678, "file:///active");
  tracker.notifyDiskFileClosed(1);

  reporter.startThreads();
  reporter.finish();
  reporter.waitThreads();

  EXPECT_EQ(std::string::npos, log.getLog().find("stillOpenFileForThread1"));
  EXPECT_EQ(std::string::npos, log.getLog().find("file:///closed"));
  EXPECT_NE(std::string::npos, log.getLog().find("stillOpenFileForThread2"));
  EXPECT_NE(std::string::npos, log.getLog().find("file:///active"));
}

TEST(TapeSessionReporterTest, ReportsAStuckFile) {
  cta::log::StringLogger log("dummy", "TapeSessionReporterTest", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  TapeSessionTracker tracker;
  ReportingTapeMount mount;
  TapeSessionReporter reporter(tracker, mount, lc, 5ms, 5ms);

  tracker.notifyBeginNewJob(1234, 42);
  reporter.startThreads();
  std::this_thread::sleep_for(20ms);
  reporter.finish();
  reporter.waitThreads();

  EXPECT_NE(std::string::npos, log.getLog().find("No tape block movement for too long"));
}

}  // namespace cta::tape::daemon
