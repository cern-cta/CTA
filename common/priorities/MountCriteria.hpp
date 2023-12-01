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
 * Class representing the criteria be met in order to justify mounting a tape.
 */
class MountCriteria {
public:

  /**
   * Constructor.
   */
  MountCriteria();

  /**
   * Constructor.
   *
   * @param nbBytes The minimum number of queued bytes required to justify a
   * mount.
   * @param nbFiles The minimum number of queued files required to justify a
   * mount.
   * @param ageInSecs The minimum age in seconds of queued data required to
   * justify a mount.
   */
  MountCriteria(const uint64_t nbBytes, const uint64_t nbFiles,
    const uint64_t ageInSecs);

  /**
   * Returns the minimum number of queued bytes required to justify a mount.
   *
   * @return The minimum number of queued bytes required to justify a mount.
   */
  uint64_t getNbBytes() const noexcept;

  /**
   * Returns the minimum number of queued files required to justify a mount.
   *
   * @return The minimum number of queued files required to justify a mount.
   */
  uint64_t getNbFiles() const noexcept;

  /**
   * Returns the minimum age in seconds of queued data required to justify a
   * mount.
   *
   * @return The minimum age in seconds of queued data required to justify a
   * mount.
   */
  uint64_t getAgeInSecs() const noexcept;

private:

  /**
   * The minimum number of queued bytes required to justify a mount.
   */
  uint64_t m_nbBytes;

  /**
   * The minimum number of queued files required to justify a mount.
   */
  uint64_t m_nbFiles;

  /**
   * The minimum age in seconds of queued data required to justify a mount.
   */
  uint64_t m_ageInSecs;

}; // class MountCriteria

} // namespace cta
