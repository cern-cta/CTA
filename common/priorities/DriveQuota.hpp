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

#include <stdint.h>

namespace cta {

/**
 * Class representing a tape-drive quota.
 */
class DriveQuota {
public:
  /**
   * Constructor.
   */
  DriveQuota();

  /**
   * Constructor.
   *
   * @param minDrives The minimum number of drives that should be provided.
   * @param maxDrives The maximum number of drives that should be provided.
   */
  DriveQuota(const uint32_t minDrives, const uint32_t maxDrives);

  /**
   * Returns the minimum number of drives that should be provided.
   *
   * @return The minimum number of drives that should be provided.
   */
  uint32_t getMinDrives() const throw();

  /**
   * Returns the maximum number of drives that should be provided.
   *
   * @return The maximum number of drives that should be provided.
   */
  uint32_t getMaxDrives() const throw();

private:
  /**
   * The minimum number of drives that should be provided.
   */
  uint32_t m_minDrives;

  /**
   * The maximum number of drives that should be provided.
   */
  uint32_t m_maxDrives;

};  // class DriveQuota

}  // namespace cta
