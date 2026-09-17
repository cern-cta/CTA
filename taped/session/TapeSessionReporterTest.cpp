/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapeSessionReporter.hpp"

#include "common/log/StringLogger.hpp"
#include "scheduler/TapeMountDummy.hpp"

#include <atomic>
#include <chrono>
#include <gtest/gtest.h>
#include <map>
#include <regex>
#include <stdexcept>
#include <string_view>
#include <thread>

namespace cta::tape::daemon {

using namespace std::chrono_literals;

size_t countMessage(const std::string& output, std::string_view message) {
  size_t count = 0;
  for (size_t pos = output.find(message); pos != std::string::npos; pos = output.find(message, pos + message.size())) {
    ++count;
  }
  return count;
}

class ReportingTapeMount : public cta::TapeMountDummy {
public:
  TapeTransferStats reportedStats;
  std::atomic<unsigned int> statsReports = 0;

  void setTapeSessionStats(const TapeTransferStats& stats) override {
    reportedStats = stats;
    ++statsReports;
  }

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
  tracker.reportState(cta::tape::session::TransferState::Transferring);

  reporter.reportNow();

  EXPECT_NE(std::string::npos, log.getLog().find("Tape session statistics"));
  EXPECT_NE(std::string::npos, log.getLog().find("Error_diskRead"));
  EXPECT_NE(std::string::npos, log.getLog().find("\"status\":\"in_progress\""));
  EXPECT_NE(std::string::npos, log.getLog().find("\"transferState\":\"Transferring\""));
  EXPECT_EQ(std::string::npos, log.getLog().find("\"sessionState\":"));
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
  EXPECT_NE(std::string::npos, output.find("\"transferState\":null"));
}

TEST(TapeSessionReporterTest, PeriodicallyReportsAndFlushesOnShutdown) {
  cta::log::StringLogger log("dummy", "TapeSessionReporterTest", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  ReportingTapeMount mount;
  TapeSessionTracker tracker;
  tracker.setMount(&mount);
  tracker.updateTapeTransferStats({.dataVolume = 42, .filesCount = 1});
  TapeSessionReporter reporter(tracker, lc, 5ms, 1s);

  reporter.startThreads();
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while (mount.statsReports.load() == 0 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  tracker.updateTapeTransferStats({.dataVolume = 84, .filesCount = 2});
  tracker.reportState(cta::tape::session::TransferState::Finished);
  reporter.finish();
  reporter.waitThreads();

  const auto output = log.getLog();
  EXPECT_NE(std::string::npos, output.find("Tape session statistics"));
  EXPECT_NE(std::string::npos, output.find("dataVolume=\"42\""));
  EXPECT_EQ(1, countMessage(output, "Tape session finished"));
  const auto finishedAt = output.find("Tape session finished");
  if (finishedAt != std::string::npos) {
    EXPECT_EQ(0, countMessage(output.substr(finishedAt), "Tape session statistics"));
  }
  EXPECT_NE(std::string::npos, output.find("tape_session_finished"));
  EXPECT_EQ(84, mount.reportedStats.dataVolume);
  EXPECT_EQ(2, mount.reportedStats.filesCount);
  EXPECT_GE(mount.statsReports.load(), 2U);
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
  tracker.reportState(cta::tape::session::TransferState::Finished);
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
  tracker.reportState(cta::tape::session::TransferState::Finished);
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
  tracker.reportState(cta::tape::session::TransferState::Finished);
  reporter.finish();
  reporter.waitThreads();

  EXPECT_NE(std::string::npos, log.getLog().find("No tape block movement for too long"));
}

TEST(TapeSessionReporterTest, MovementAndCompletionStopStuckFileWarnings) {
  cta::log::StringLogger log("dummy", "TapeSessionReporterTest", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  ReportingTapeMount mount;
  TapeSessionTracker tracker;
  tracker.setMount(&mount);
  TapeSessionReporter reporter(tracker, lc, 5ms, 20ms);

  tracker.notifyBeginNewJob(1234, 42);
  reporter.startThreads();
  // Wait for the first stuck warning without reading the logger concurrently.
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while (mount.statsReports.load() < 5 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  tracker.notifyBlockMovement(100);
  std::this_thread::sleep_for(5ms);
  tracker.fileFinished();
  std::this_thread::sleep_for(25ms);
  tracker.reportState(cta::tape::session::TransferState::Finished);
  reporter.finish();
  reporter.waitThreads();

  EXPECT_EQ(1, countMessage(log.getLog(), "No tape block movement for too long"));
}

TEST(TapeSessionReporterTest, StoppingReporterDoesNotClaimTransferCompletion) {
  cta::log::StringLogger log("dummy", "TapeSessionReporterTest", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  ReportingTapeMount mount;
  TapeSessionTracker tracker;
  tracker.setMount(&mount);
  tracker.beginTransfer();
  tracker.setOutcome(TapeSessionOutcome::Failure);
  TapeSessionReporter reporter(tracker, lc, 1s, 1s);
  reporter.startThreads();
  reporter.finish();
  reporter.waitThreads();

  EXPECT_EQ(cta::tape::session::TransferState::Preparing, tracker.state());
  EXPECT_EQ(0, countMessage(log.getLog(), "Tape session finished"));
}

TEST(TapeSessionReporterTest, FinalPublicationFailureOverridesSuccessfulOutcome) {
  class FailingMount : public ReportingTapeMount {
  public:
    void setTapeSessionStats(const TapeTransferStats&) override {
      ++statsReports;
      throw std::runtime_error("injected final publication failure");
    }
  } mount;

  cta::log::StringLogger log("dummy", "TapeSessionReporterTest", cta::log::DEBUG);
  log.setLogFormat("json");
  cta::log::LogContext lc(log);
  TapeSessionTracker tracker;
  tracker.setMount(&mount);
  tracker.beginTransfer();
  tracker.setOutcome(TapeSessionOutcome::Success);
  tracker.reportState(cta::tape::session::TransferState::Finished);
  TapeSessionReporter reporter(tracker, lc, 1s, 1s);
  reporter.startThreads();
  reporter.finish();
  reporter.waitThreads();

  EXPECT_EQ(1U, mount.statsReports.load());
  EXPECT_EQ(TapeSessionOutcome::Failure, tracker.outcome());
  EXPECT_EQ(1, tracker.errorStats().at(TapeSessionError::Reporting));
  EXPECT_EQ(1, countMessage(log.getLog(), "Tape session finished"));
  EXPECT_NE(std::string::npos, log.getLog().find("\"transferState\":\"Finished\""));
  EXPECT_NE(std::string::npos, log.getLog().find("\"status\":\"failure\""));
  EXPECT_EQ(std::string::npos, log.getLog().find("\"status\":\"success\""));
}

TEST(TapeSessionReporterTest, SuccessfulEmptyMountHasAFinalSuccessOutcome) {
  cta::log::StringLogger log("dummy", "TapeSessionReporterTest", cta::log::DEBUG);
  log.setLogFormat("json");
  cta::log::LogContext lc(log);
  ReportingTapeMount mount;
  TapeSessionTracker tracker;
  tracker.setMount(&mount);
  tracker.beginTransfer();
  tracker.incrementError(TapeSessionError::EmptyMount);
  tracker.setOutcome(TapeSessionOutcome::Success);
  tracker.reportState(cta::tape::session::TransferState::Finished);
  TapeSessionReporter reporter(tracker, lc, 1s, 1s);
  reporter.startThreads();
  reporter.finish();
  reporter.waitThreads();

  EXPECT_EQ(1, countMessage(log.getLog(), "Tape session finished"));
  EXPECT_NE(std::string::npos, log.getLog().find("\"status\":\"success\""));
  EXPECT_EQ(std::string::npos, log.getLog().find("\"status\":\"in_progress\""));
}

TEST(TapeSessionReporterTest, AutomaticOutcomeIsOnlyReportedAtCompletion) {
  for (bool failed : {false, true}) {
    cta::log::StringLogger log("dummy", "TapeSessionReporterTest", cta::log::DEBUG);
    log.setLogFormat("json");
    cta::log::LogContext lc(log);
    ReportingTapeMount mount;
    TapeSessionTracker tracker;
    tracker.setMount(&mount);
    tracker.beginTransfer();
    if (failed) {
      tracker.incrementError(TapeSessionError::DiskWrite);
    }
    TapeSessionReporter reporter(tracker, lc, 1s, 1s);
    reporter.reportNow();
    const auto periodic = log.getLog();
    EXPECT_NE(std::string::npos, periodic.find("\"status\":\"in_progress\""));
    EXPECT_EQ(std::string::npos, periodic.find("\"status\":\"success\""));
    EXPECT_EQ(std::string::npos, periodic.find("\"status\":\"failure\""));

    tracker.reportState(cta::tape::session::TransferState::Finished);
    reporter.startThreads();
    reporter.finish();
    reporter.waitThreads();
    const auto finalOutput = log.getLog().substr(periodic.size());
    EXPECT_EQ(1, countMessage(finalOutput, "Tape session finished"));
    EXPECT_NE(std::string::npos, finalOutput.find(failed ? "\"status\":\"failure\"" : "\"status\":\"success\""));
  }
}

}  // namespace cta::tape::daemon
