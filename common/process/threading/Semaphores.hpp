/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-FileCopyrightText: 2026 DESY
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <chrono>
#include <semaphore>
#include <stdint.h>

namespace cta::threading {

/**
 * A thread-safe counting semaphore wrapper around C++20 std::counting_semaphore.
 * Provides exception-based error handling and convenient timeout support.
 */
class CountingSemaphore {
public:
  class Timeout : public std::exception {
  public:
    const char* what() const noexcept override { return "Semaphore timeout"; }
  };

  explicit CountingSemaphore(int initial = 0);
  ~CountingSemaphore() = default;

  // Delete copy semantics
  CountingSemaphore(const CountingSemaphore&) = delete;
  CountingSemaphore& operator=(const CountingSemaphore&) = delete;

  // Allow move semantics
  CountingSemaphore(CountingSemaphore&&) = default;
  CountingSemaphore& operator=(CountingSemaphore&&) = default;

  /**
   * Blocking acquire. Waits until the semaphore count becomes positive.
   */
  void acquire();

  /**
   * Blocking acquire with timeout.
   * @param timeout_us Timeout in microseconds
   * @throws Timeout if the timeout expires
   */
  void acquireWithTimeout(uint64_t timeout_us);

  /**
   * Non-blocking acquire attempt.
   * @return true if acquire succeeded, false if semaphore count is zero
   */
  bool tryAcquire();

  /**
   * Release the semaphore, incrementing its count.
   * @param n Number of times to release (default: 1)
   */
  void release(int n = 1);

private:
  // Use INT_MAX (2^31-1) as semaphore limit, which is the practical maximum
  // for most implementations and well within std::counting_semaphore limits
  std::counting_semaphore<INT_MAX> m_sem;
};

}  // namespace cta::threading
