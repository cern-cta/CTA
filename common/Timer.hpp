/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
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
  enum reset_t {
    keepRunning,
    resetCounter
  };

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

}; // class Timer

} // namespace cta::utils
