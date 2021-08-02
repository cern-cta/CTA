/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
#include "Timer.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::utils::Timer::Timer() {
  reset();
}

//------------------------------------------------------------------------------
// usecs
//------------------------------------------------------------------------------
int64_t cta::utils::Timer::usecs(reset_t reset) {
  timeval now;
  gettimeofday(&now, 0);
  int64_t ret = ((now.tv_sec * 1000000) + now.tv_usec) - 
                 ((m_reference.tv_sec * 1000000) + m_reference.tv_usec);
  if (reset == resetCounter) {
    m_reference = now;
  }
  return ret;
}

//------------------------------------------------------------------------------
// secs
//------------------------------------------------------------------------------
double cta::utils::Timer::secs(reset_t reset) {
  return usecs(reset) * 0.000001;
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void cta::utils::Timer::reset() {
  gettimeofday(&m_reference, 0);
}

