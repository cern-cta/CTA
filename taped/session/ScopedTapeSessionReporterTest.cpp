/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "ScopedTapeSessionReporter.hpp"

#include "common/log/LogContext.hpp"
#include "common/log/Logger.hpp"

#include <cstdio>
#include <cstdlib>
#include <gtest/gtest.h>
#include <stdexcept>

namespace cta::tape::daemon {
namespace {

// Death tests capture stderr; write synchronously before the process exits.
class FatalTestLogger : public log::Logger {
public:
  FatalTestLogger() : Logger("host", "taped", log::DEBUG) {}

  void refresh() override {}

  bool fail = false;

private:
  void writeMsgToUnderlyingLoggingSystem(std::string_view header, std::string_view body) override {
    if (fail) {
      throw std::runtime_error("logging failed");
    }
    std::fprintf(stderr, "%.*s%.*s\n", int(header.size()), header.data(), int(body.size()), body.data());
    std::fflush(stderr);
  }
};

// Distinct exit codes make accidental cleanup observable in the child process.
struct BorrowedResources {
  ~BorrowedResources() { std::_Exit(80); }
};

struct TestReporter {
  void startThreads() {
    ++starts;
    if (failStart) {
      throw std::runtime_error("reporter startup failed");
    }
  }

  void finish() {
    ++stops;
    if (failStop) {
      throw std::runtime_error("reporter stop failed");
    }
  }

  void waitThreads() {
    ++joins;
    if (failJoin) {
      throw std::runtime_error("reporter join failed");
    }
  }

  int starts = 0;
  int stops = 0;
  int joins = 0;
  bool failJoin = false;
  bool failStart = false;
  bool failStop = false;
};

class ScopedTapeSessionReporterDeathTest : public testing::Test {
protected:
  FatalTestLogger logger;
  log::LogContext lc {logger};
};

TEST_F(ScopedTapeSessionReporterDeathTest, ExplicitReporterJoinFailureExitsImmediately) {
  ASSERT_EXIT(
    {
      BorrowedResources resources;
      TestReporter reporter;
      reporter.failJoin = true;
      ScopedTapeSessionReporter guard(reporter, lc);
      guard.start();
      guard.finish();
      std::_Exit(82);
    },
    testing::ExitedWithCode(EXIT_FAILURE),
    "CRIT.*Reporter termination could not be established.*protect borrowed session state.*reporter join failed");
}

TEST_F(ScopedTapeSessionReporterDeathTest, ReporterJoinFailureDuringUnwindingExitsImmediately) {
  ASSERT_EXIT(
    {
      BorrowedResources resources;
      TestReporter reporter;
      reporter.failJoin = true;
      try {
        ScopedTapeSessionReporter guard(reporter, lc);
        guard.start();
        throw std::runtime_error("session failure");
      } catch (...) {
        std::_Exit(82);
      }
    },
    testing::ExitedWithCode(EXIT_FAILURE),
    "CRIT.*Reporter termination could not be established.*protect borrowed session state.*reporter join failed");
}

TEST_F(ScopedTapeSessionReporterDeathTest, ReporterIsJoinedOnlyOnce) {
  TestReporter reporter;
  {
    ScopedTapeSessionReporter guard(reporter, lc);
    guard.start();
    guard.finish();
  }
  EXPECT_EQ(1, reporter.starts);
  EXPECT_EQ(1, reporter.stops);
  EXPECT_EQ(1, reporter.joins);
}

TEST_F(ScopedTapeSessionReporterDeathTest, StartupFailureExitsWithoutCleanup) {
  ASSERT_EXIT(
    {
      BorrowedResources resources;
      TestReporter reporter;
      reporter.failStart = true;
      ScopedTapeSessionReporter guard(reporter, lc);
      guard.start();
    },
    testing::ExitedWithCode(EXIT_FAILURE),
    "CRIT.*Reporter termination could not be established.*reporter startup failed");
}

TEST_F(ScopedTapeSessionReporterDeathTest, StopFailureExitsWithoutCleanup) {
  ASSERT_EXIT(
    {
      BorrowedResources resources;
      TestReporter reporter;
      reporter.failStop = true;
      ScopedTapeSessionReporter guard(reporter, lc);
      guard.start();
      guard.finish();
    },
    testing::ExitedWithCode(EXIT_FAILURE),
    "CRIT.*Reporter termination could not be established.*reporter stop failed");
}

TEST_F(ScopedTapeSessionReporterDeathTest, LoggingFailureStillExitsWithoutCleanup) {
  ASSERT_EXIT(
    {
      BorrowedResources resources;
      std::atexit([] { std::_Exit(81); });
      logger.fail = true;
      TestReporter reporter;
      reporter.failJoin = true;
      ScopedTapeSessionReporter guard(reporter, lc);
      guard.start();
      guard.finish();
    },
    testing::ExitedWithCode(EXIT_FAILURE),
    "");
}

TEST_F(ScopedTapeSessionReporterDeathTest, DestructorJoinsStartedReporter) {
  TestReporter reporter;
  {
    ScopedTapeSessionReporter guard(reporter, lc);
    guard.start();
  }
  EXPECT_EQ(1, reporter.stops);
  EXPECT_EQ(1, reporter.joins);
}

TEST_F(ScopedTapeSessionReporterDeathTest, UnstartedReporterNeedsNoJoin) {
  TestReporter reporter;
  { ScopedTapeSessionReporter guard(reporter, lc); }
  EXPECT_EQ(0, reporter.stops);
  EXPECT_EQ(0, reporter.joins);
}

}  // namespace
}  // namespace cta::tape::daemon
