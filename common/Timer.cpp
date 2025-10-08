/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "Timer.hpp"

namespace cta::utils {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Timer::Timer() {
  reset();
}

//------------------------------------------------------------------------------
// usecs
//------------------------------------------------------------------------------
int64_t Timer::usecs(reset_t reset) {
  auto now = std::chrono::steady_clock::now();
  int64_t elapsed = std::chrono::duration_cast<std::chrono::microseconds>(now - m_reference).count();
  if (reset == resetCounter) {
    m_reference = now;
  }
  return elapsed;
}

//------------------------------------------------------------------------------
// msecs
//------------------------------------------------------------------------------
int64_t Timer::msecs(reset_t reset) {
  auto now = std::chrono::steady_clock::now();
  int64_t elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - m_reference).count();
  if (reset == resetCounter) {
    m_reference = now;
  }
  return elapsed;
}

//------------------------------------------------------------------------------
// secs
//------------------------------------------------------------------------------
double Timer::secs(reset_t reset) {
  return static_cast<double>(usecs(reset)) * 1.0e-6;  // Convert microseconds to seconds
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void Timer::reset() {
  m_reference = std::chrono::steady_clock::now();
}

}  // namespace cta::utils
