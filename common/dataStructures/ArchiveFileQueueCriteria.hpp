/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/dataStructures/MountPolicy.hpp"
#include "common/dataStructures/TapeCopyToPoolMap.hpp"

#include <stdint.h>

namespace cta {
namespace common {
namespace dataStructures {

/**
 * The queueing criteria created after preparing for a new archive file within
 * the Catalogue.  This queueing criteria specify on which tape-pool queue(s)
 * the corresponding data-transfer jobs are to be queued and which mount policy
 * should be used.
 */
struct ArchiveFileQueueCriteria {

  /**
   * Constructor.
   */
  ArchiveFileQueueCriteria();

  /**
   * Constructor.
   *
   * @param fileId The unique archive-file identifier.
   * @param copyToPoolMap The map from tape copy number to tape pool name.
   * @param mountPolicy The mount policy.
   */
  ArchiveFileQueueCriteria(
    const uint64_t fileId,
    const TapeCopyToPoolMap &copyToPoolMap,
    const MountPolicy &mountPolicy);

  // TODO: rename to archiveFileId?
  /**
   * The unique archive-file identifier.
   */
  uint64_t fileId;

  /**
   * The map from tape copy number to tape pool name.
   */
  TapeCopyToPoolMap copyToPoolMap;

  /**
   * The mount policy.
   */
  MountPolicy mountPolicy;

}; // struct ArchiveFileQueueCriteria

} // namespace dataStructures
} // namespace common
} // namespace cta
