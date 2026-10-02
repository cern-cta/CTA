/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "WorkerFailureHandling.hpp"

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

class WorkerFailureHandlingDeathTest : public testing::Test {
protected:
  FatalTestLogger logger;
  log::LogContext lc {logger};
};

TEST_F(WorkerFailureHandlingDeathTest, PartialStartupExitsBeforeResourceDestructionOrExitHandlers) {
  ASSERT_EXIT(
    {
      BorrowedResources resources;
      std::atexit([] { std::_Exit(81); });
      lc.push(log::Param("tapeDrive", "drive"));
      lc.push(log::Param("tapeVid", "V00001"));
      lc.push(log::Param("mountId", "123"));
      bool firstWorkerStarted = false;
      runOrExitOnWorkerFailure(lc, [&] {
        firstWorkerStarted = true;
        throw std::runtime_error("second worker startup failed");
      });
      std::_Exit(firstWorkerStarted ? 82 : 83);
    },
    testing::ExitedWithCode(EXIT_FAILURE),
    "CRIT.*Tape session worker termination could not be established.*second worker startup "
    "failed.*mountId.*123.*tapeDrive.*drive.*tapeVid.*V00001");
}

TEST_F(WorkerFailureHandlingDeathTest, JoinFailureExitsBeforeRemainingResourcesUnwind) {
  ASSERT_EXIT(
    {
      BorrowedResources resources;
      runOrExitOnWorkerFailure(lc, [] { throw std::runtime_error("worker join failed"); });
      std::_Exit(82);
    },
    testing::ExitedWithCode(EXIT_FAILURE),
    "CRIT.*worker join failed");
}

TEST_F(WorkerFailureHandlingDeathTest, NonStandardFailureExitsWithDiagnostic) {
  ASSERT_EXIT(runOrExitOnWorkerFailure(lc, [] { throw 42; }),
              testing::ExitedWithCode(EXIT_FAILURE),
              "CRIT.*Unknown exception");
}

TEST_F(WorkerFailureHandlingDeathTest, LoggingFailureStillExitsWithoutCleanup) {
  ASSERT_EXIT(
    {
      BorrowedResources resources;
      logger.fail = true;
      runOrExitOnWorkerFailure(lc, [] { throw std::runtime_error("worker join failed"); });
    },
    testing::ExitedWithCode(EXIT_FAILURE),
    "");
}

TEST_F(WorkerFailureHandlingDeathTest, SuccessfulOperationReturnsNormally) {
  bool joined = false;
  runOrExitOnWorkerFailure(lc, [&] { joined = true; });
  EXPECT_TRUE(joined);
}

}  // namespace
}  // namespace cta::tape::daemon
