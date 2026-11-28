/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/utils/RetryWithBackoff.hpp"

#include "common/log/DummyLogger.hpp"
#include "common/log/LogContext.hpp"

#include <chrono>
#include <gtest/gtest.h>

namespace cta::utils {
namespace {

class RetryWithBackoffTest : public ::testing::Test {
protected:
  cta::log::DummyLogger m_logger {"dummy", "retryWithBackoffTest"};
  cta::log::LogContext m_lc {m_logger};
  // Real but negligible sleeps: the delay list's length is what the retry logic keys on.
  const std::vector<std::chrono::milliseconds> m_twoDelays {std::chrono::milliseconds(1), std::chrono::milliseconds(1)};
};

TEST_F(RetryWithBackoffTest, SucceedsFirstAttemptWithoutRetrying) {
  int attempts = 0;
  retryWithBackoff([&] { ++attempts; }, m_twoDelays, m_lc, "test");
  ASSERT_EQ(1, attempts);
}

TEST_F(RetryWithBackoffTest, RetriesAfterEachFailureThenSucceeds) {
  int attempts = 0;
  retryWithBackoff(
    [&] {
      if (++attempts < 3) {
        throw cta::exception::Exception("transient failure");
      }
    },
    m_twoDelays,
    m_lc,
    "test");
  // One attempt per delay plus the initial one: the third attempt succeeds.
  ASSERT_EQ(3, attempts);
}

TEST_F(RetryWithBackoffTest, RethrowsTheLastExceptionOnceAttemptsAreExhausted) {
  int attempts = 0;
  ASSERT_THROW(retryWithBackoff(
                 [&] {
                   ++attempts;
                   throw cta::exception::Exception("persistent failure " + std::to_string(attempts));
                 },
                 m_twoDelays,
                 m_lc,
                 "test"),
               cta::exception::Exception);
  // Initial attempt + one per delay, then the LAST exception propagates.
  ASSERT_EQ(3, attempts);
  try {
    retryWithBackoff([&] { throw cta::exception::Exception("persistent failure again"); }, {}, m_lc, "test");
    FAIL() << "expected cta::exception::Exception";
  } catch (const cta::exception::Exception& ex) {
    ASSERT_EQ("persistent failure again", ex.getMessageValue());
  }
}

TEST_F(RetryWithBackoffTest, NonCtaExceptionPropagatesWithoutRetrying) {
  // Only cta::exception::Exception marks an attempt as retryable: anything else is a programming
  // error and must surface immediately, not after the full backoff schedule.
  int attempts = 0;
  ASSERT_THROW(retryWithBackoff(
                 [&] {
                   ++attempts;
                   throw std::runtime_error("not a CTA exception");
                 },
                 m_twoDelays,
                 m_lc,
                 "test"),
               std::runtime_error);
  ASSERT_EQ(1, attempts);
}

}  // namespace
}  // namespace cta::utils
