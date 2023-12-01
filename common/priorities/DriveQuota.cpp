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
uint32_t cta::DriveQuota::getMinDrives() const noexcept {
  return m_minDrives;
}

//------------------------------------------------------------------------------
// getMaxDrives
//------------------------------------------------------------------------------
uint32_t cta::DriveQuota::getMaxDrives() const noexcept {
  return m_maxDrives;
}
