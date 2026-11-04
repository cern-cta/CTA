/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "runtime/SafeRun.hpp"

#include "common/log/StringLogger.hpp"
#include "common/utils/ScopeExit.hpp"

#include <gtest/gtest.h>
#include <stdexcept>
#include <streambuf>

namespace unitTests {

static_assert(noexcept(cta::runtime::safeRun([] { return 0; })));
static_assert(noexcept(cta::runtime::safeRunWithLog(std::declval<cta::log::Logger&>(), [] { return 0; })));

TEST(SafeRun, PreservesReturnCode) {
  cta::log::StringLogger log("localhost", "test", cta::log::DEBUG);
  EXPECT_EQ(42, cta::runtime::safeRun([] { return 42; }));
  EXPECT_EQ(42, cta::runtime::safeRunWithLog(log, [] { return 42; }));
}

TEST(SafeRun, ReturnsFailureWhenStderrReportingThrows) {
  class FailingBuffer : public std::streambuf {
    int_type overflow(int_type) override { throw std::runtime_error("stderr unavailable"); }
  } buffer;

  // Capture the file descriptor too: the fallback must bypass the broken C++ stream.
  testing::internal::CaptureStderr();
  // Restore the global stream before the test framework reports any assertions.
  int result;
  {
    const auto oldExceptions = std::cerr.exceptions();
    const auto oldState = std::cerr.rdstate();
    auto* oldBuffer = std::cerr.rdbuf();
    const cta::utils::ScopeExit restore([&] {
      std::cerr.exceptions(std::ios::goodbit);
      std::cerr.rdbuf(oldBuffer);
      std::cerr.clear(oldState);
      std::cerr.exceptions(oldExceptions);
    });
    std::cerr.rdbuf(&buffer);
    std::cerr.exceptions(std::ios::badbit | std::ios::failbit);
    result = cta::runtime::safeRun([]() -> int { throw std::runtime_error("application failure"); });
  }
  const auto diagnostic = testing::internal::GetCapturedStderr();
  EXPECT_EQ(EXIT_FAILURE, result);
  EXPECT_NE(std::string::npos, diagnostic.find("unable to report exception details"));
}

TEST(SafeRun, LogsStandardAndUnknownExceptions) {
  cta::log::StringLogger log("localhost", "test", cta::log::DEBUG);
  EXPECT_EQ(EXIT_FAILURE,
            cta::runtime::safeRunWithLog(log, []() -> int { throw std::runtime_error("application failure"); }));
  EXPECT_NE(std::string::npos, log.getLog().find("application failure"));
  log.clearLog();
  EXPECT_EQ(EXIT_FAILURE, cta::runtime::safeRunWithLog(log, []() -> int { throw 42; }));
  EXPECT_NE(std::string::npos, log.getLog().find("Fatal unknown exception"));
}

}  // namespace unitTests
