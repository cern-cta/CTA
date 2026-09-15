/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapeSessionReporter.hpp"

#include "common/log/StringLogger.hpp"
#include "scheduler/TapeMountDummy.hpp"

#include <chrono>
#include <gtest/gtest.h>
#include <map>
#include <regex>
#include <thread>

namespace cta::tape::daemon {

using namespace std::chrono_literals;

class ReportingTapeMount : public cta::TapeMountDummy {
public:
  TapeTransferStats reportedStats;

  void setTapeSessionStats(const TapeTransferStats& stats) override { reportedStats = stats; }

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
  log.setLogFormat("json");
  cta::log::LogContext lc(log);
  ReportingTapeMount mount;
  TapeSessionTracker tracker;
  tracker.setMount(&mount);
  TapeSessionReporter reporter(tracker, lc, 1s, 1s);

  tracker.notifyBlockMovement(25);
  tracker.incrementError(TapeSessionError::DiskRead);

  reporter.reportNow();

  EXPECT_NE(std::string::npos, log.getLog().find("Tape session statistics"));
  EXPECT_NE(std::string::npos, log.getLog().find("Error_diskRead"));
  EXPECT_NE(std::string::npos, log.getLog().find("\"status\":\"failure\""));
}

TEST(TapeSessionReporterTest, ReportsSplitStatsWithExistingFieldNamesAndCalculations) {
  cta::log::StringLogger log("dummy", "TapeSessionReporterTest", cta::log::DEBUG);
  log.setLogFormat("json");
  cta::log::LogContext lc(log);
  ReportingTapeMount mount;
  TapeSessionTracker tracker;
  tracker.setMount(&mount);
  tracker.updateTapeSetupStats(
    {.mountTime = 1, .initialMountTime = 2, .tapeLoadTime = 3, .encryptionControlTime = 5, .positionTime = 6});
  tracker.updateTapeTransferStats({.positionTime = 7, .readWriteTime = 4, .dataVolume = 10000000, .filesCount = 2});
  tracker.updateDiskTransferStats({.deliveryTime = 15, .waitReportingTime = 2});
  tracker.addTapeCleanupStats({.unloadTime = 1,
                               .unmountTime = 2,
                               .cleanupTime = 7,
                               .lbpResetTime = 1,
                               .readinessWaitTime = 1,
                               .rewindTime = 1,
                               .labelReadTime = 1,
                               .encryptionControlTime = 3});
  tracker.setTotalTime(10);
  TapeSessionReporter reporter(tracker, lc, 1s, 1s);

  reporter.reportNow();

  const auto output = log.getLog();
  for (const auto& [field, value] : std::map<std::string, unsigned> {
         {"mountTime",                1 },
         {"initialMountTime",         2 },
         {"tapeLoadTime",             3 },
         {"positionTime",             13},
         {"unloadTime",               1 },
         {"unmountTime",              2 },
         {"cleanupTime",              7 },
         {"lbpResetTime",             1 },
         {"readinessWaitTime",        1 },
         {"rewindTime",               1 },
         {"labelReadTime",            1 },
         {"encryptionControlTime",    8 },
         {"transferTime",             4 },
         {"totalTime",                10},
         {"deliveryTime",             15},
         {"drainingTime",             5 },
         {"payloadTransferSpeedMBps", 1 }
  }) {
    EXPECT_TRUE(
      std::regex_search(output, std::regex("\"" + field + "\":" + std::to_string(value) + R"((?:\.0+)?[,}])")))
      << field;
  }
  EXPECT_EQ(10000000, mount.reportedStats.dataVolume);
  EXPECT_EQ(2, mount.reportedStats.filesCount);
  EXPECT_EQ(7, mount.reportedStats.positionTime);
}

TEST(TapeSessionReporterTest, PeriodicallyReportsAndFlushesOnShutdown) {
  cta::log::StringLogger log("dummy", "TapeSessionReporterTest", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  ReportingTapeMount mount;
  TapeSessionTracker tracker;
  tracker.setMount(&mount);
  TapeSessionReporter reporter(tracker, lc, 5ms, 1s);

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
  log.setLogFormat("json");
  cta::log::LogContext lc(log);
  ReportingTapeMount mount;
  TapeSessionTracker tracker;
  tracker.setMount(&mount);
  TapeSessionReporter reporter(tracker, lc, 1s, 1s);

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
  ReportingTapeMount mount;
  TapeSessionTracker tracker;
  tracker.setMount(&mount);
  TapeSessionReporter reporter(tracker, lc, 1s, 1s);

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
  ReportingTapeMount mount;
  TapeSessionTracker tracker;
  tracker.setMount(&mount);
  TapeSessionReporter reporter(tracker, lc, 5ms, 5ms);

  tracker.notifyBeginNewJob(1234, 42);
  reporter.startThreads();
  std::this_thread::sleep_for(20ms);
  reporter.finish();
  reporter.waitThreads();

  EXPECT_NE(std::string::npos, log.getLog().find("No tape block movement for too long"));
}

}  // namespace cta::tape::daemon
