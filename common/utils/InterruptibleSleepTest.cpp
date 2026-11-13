/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/utils/InterruptibleSleep.hpp"

#include <future>
#include <gtest/gtest.h>

namespace cta::utils {
namespace {
using namespace std::chrono_literals;

TEST(InterruptibleSleep, WaitsForDurationWithoutStop) {
  const auto start = std::chrono::steady_clock::now();
  interruptibleSleep({}, 20ms);
  EXPECT_GE(std::chrono::steady_clock::now() - start, 20ms);
}

TEST(InterruptibleSleep, ReturnsWhenAlreadyStopped) {
  std::stop_source source;
  source.request_stop();
  const auto start = std::chrono::steady_clock::now();
  interruptibleSleep(source.get_token(), 2s);
  EXPECT_LT(std::chrono::steady_clock::now() - start, 1s);
}

TEST(InterruptibleSleep, StopWakesWaitingThread) {
  std::stop_source source;
  auto waiting = std::async(std::launch::async, [&] { interruptibleSleep(source.get_token(), 2s); });

  // Confirm the delay has not completed before requesting stop.
  EXPECT_EQ(waiting.wait_for(20ms), std::future_status::timeout);
  source.request_stop();
  EXPECT_EQ(waiting.wait_for(1s), std::future_status::ready);
  waiting.get();
}

}  // namespace
}  // namespace cta::utils
