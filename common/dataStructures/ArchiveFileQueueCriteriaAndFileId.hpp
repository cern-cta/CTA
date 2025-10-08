/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
struct ArchiveFileQueueCriteriaAndFileId {

  /**
   * Constructor.
   */
  ArchiveFileQueueCriteriaAndFileId();

  /**
   * Constructor.
   *
   * @param fileId The unique archive-file identifier.
   * @param copyToPoolMap The map from tape copy number to tape pool name.
   * @param mountPolicy The mount policy.
   */
  ArchiveFileQueueCriteriaAndFileId(
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

}; // struct ArchiveFileQueueCriteriaAndFileId

} // namespace cta::common::dataStructures
