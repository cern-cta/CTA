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

namespace cta {
/**
   * Description of the criteria to mount a tape
   */
struct MountCriteria {
  uint64_t maxFilesQueued; /**< The maximum number of files to be queued 
                              * before trigerring a mount */
  uint64_t maxBytesQueued; /**< The maximum amount a data before trigerring
                           * a request */
  uint64_t maxAge;         /**< The maximum age for a request before trigerring
                           * a request (in seconds) */
  uint16_t quota;          /**< The maximum number of mounts for this tape pool */

  MountCriteria(uint64_t mf, uint64_t mb, uint64_t ma, uint16_t q) :
  maxFilesQueued(mf),
  maxBytesQueued(mb),
  maxAge(ma),
  quota(q) {}

  MountCriteria() : maxFilesQueued(0), maxBytesQueued(0), maxAge(0), quota(0) {}
};

struct MountCriteriaByDirection {
  MountCriteria archive;
  MountCriteria retrieve;

  MountCriteriaByDirection(const MountCriteria& a, const MountCriteria& r) : archive(a), retrieve(r) {}

  MountCriteriaByDirection() : archive(), retrieve() {}
};
}  // namespace cta