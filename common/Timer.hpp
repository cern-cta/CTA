/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <chrono>

namespace cta::utils {

/**
 * A small timing class using std::chrono for high-resolution monotonic timing.
 * It remembers a reference time (by default the time of construction)
 * and gives the elapsed time since then.
 * The reset method allows resetting the reference time to the current time.
 */
class Timer {
public:
  enum reset_t { keepRunning, resetCounter };

  /**
   * Constructor
   */
  Timer();

  /**
   * Destructor
   */
  virtual ~Timer() = default;

  /**
   * Returns the elapsed time in microseconds since the reference time.
   * Optionally resets the reference time.
   */
  int64_t usecs(reset_t reset = keepRunning);

  /**
   * Returns the elapsed time in milliseconds since the reference time.
   * Optionally resets the reference time.
   */
  int64_t msecs(reset_t reset = keepRunning);

  /**
   * Returns the elapsed time in seconds (with microsecond precision).
   * Optionally resets the reference time.
   */
  double secs(reset_t reset = keepRunning);

  /**
   * Resets the Timer's reference time to the current time.
   */
  void reset();

private:
  // High-resolution, monotonic time point
  std::chrono::steady_clock::time_point m_reference;

};  // class Timer

}  // namespace cta::utils
