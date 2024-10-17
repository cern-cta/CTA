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
// secs
//------------------------------------------------------------------------------
double Timer::secs(reset_t reset) {
  return usecs(reset) * 1.0e-6;  // Convert microseconds to seconds
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void Timer::reset() {
  m_reference = std::chrono::steady_clock::now();
}

} // namespace cta::utils
