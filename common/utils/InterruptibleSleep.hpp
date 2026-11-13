/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <stop_token>

namespace cta::utils {

/// Wait for the duration to elapse or for stop to be requested, including before entry.
inline void interruptibleSleep(std::stop_token stopToken, std::chrono::milliseconds duration) {
  std::mutex mutex;
  std::condition_variable_any condition;
  std::unique_lock lock(mutex);
  // The stop-aware wait handles notification races; the predicate keeps spurious wakeups from ending the delay.
  condition.wait_for(lock, stopToken, duration, [] { return false; });
}

}  // namespace cta::utils
