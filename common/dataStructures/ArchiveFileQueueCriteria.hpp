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

#include "common/dataStructures/MountPolicy.hpp"
#include "common/dataStructures/TapeCopyToPoolMap.hpp"

#include <stdint.h>

namespace cta::common::dataStructures {

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
   * @param copyToPoolMap The map from tape copy number to tape pool name.
   * @param mountPolicy The mount policy.
   */
  ArchiveFileQueueCriteria(
    const TapeCopyToPoolMap &copyToPoolMap,
    const MountPolicy &mountPolicy);

  /**
   * The map from tape copy number to tape pool name.
   */
  TapeCopyToPoolMap copyToPoolMap;

  /**
   * The mount policy.
   */
  MountPolicy mountPolicy;

}; // struct ArchiveFileQueueCriteria

} // namespace cta::common::dataStructures
