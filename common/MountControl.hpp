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

#pragma once

namespace cta {
  /**
   * Description of the criteria to mount a tape
   */
  struct MountCriteria {
    uint64_t maxFilesQueued; /**< The maximum number of files to be queued 
                              * before trigerring a mount */
    uint64_t maxBytesQueued; /**< The maximum amount a data before trigerring
                           * a request */
    uint64_t maxAge; /**< The maximum age for a request before trigerring
                           * a request (in seconds) */
    uint16_t quota; /**< The maximum number of mounts for this tape pool */
    MountCriteria(uint64_t mf, uint64_t mb, uint64_t ma, 
      uint16_t q):
    maxFilesQueued(mf), maxBytesQueued(mb), maxAge(ma), quota(q) {}
    MountCriteria(): maxFilesQueued(0), maxBytesQueued(0), maxAge(0), quota(0) {}
  };
  
  struct MountCriteriaByDirection {
    MountCriteria archive;
    MountCriteria retrieve;
    MountCriteriaByDirection(const MountCriteria& a, const MountCriteria & r):
      archive(a), retrieve(r) {}
    MountCriteriaByDirection(): archive(), retrieve() {}
  };
}