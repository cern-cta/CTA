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

#include "common/priorities/DriveQuota.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::DriveQuota::DriveQuota():
  m_minDrives(0),
  m_maxDrives(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::DriveQuota::DriveQuota(const uint32_t minDrives, const uint32_t maxDrives):
  m_minDrives(minDrives),
  m_maxDrives(maxDrives) {
}

//------------------------------------------------------------------------------
// getMinDrives
//------------------------------------------------------------------------------
uint32_t cta::DriveQuota::getMinDrives() const throw() {
  return m_minDrives;
}

//------------------------------------------------------------------------------
// getMaxDrives
//------------------------------------------------------------------------------
uint32_t cta::DriveQuota::getMaxDrives() const throw() {
  return m_maxDrives;
}
