/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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

  MountCriteria(uint64_t mf, uint64_t mb, uint64_t ma, uint16_t q)
      : maxFilesQueued(mf),
        maxBytesQueued(mb),
        maxAge(ma),
        quota(q) {}

  MountCriteria() : maxFilesQueued(0), maxBytesQueued(0), maxAge(0), quota(0) {}
};

struct MountCriteriaByDirection {
  MountCriteria archive;
  MountCriteria retrieve;

  MountCriteriaByDirection(const MountCriteria& a, const MountCriteria& r) : archive(a), retrieve(r) {}

  MountCriteriaByDirection() {}
};
}  // namespace cta
