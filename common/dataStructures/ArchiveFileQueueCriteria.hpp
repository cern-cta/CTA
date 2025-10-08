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
struct ArchiveFileQueueCriteria {
  /**
   * Constructor
   */
  ArchiveFileQueueCriteria() = default;

  /**
   * Constructor
   *
   * @param ctpm    The map from tape copy number to tape pool name
   * @param mp      The mount policy
   */
  ArchiveFileQueueCriteria(const TapeCopyToPoolMap& ctpm, const MountPolicy& mp) :
    copyToPoolMap(ctpm),
    mountPolicy(mp) { }

  /**
   * The map from tape copy number to tape pool name.
   */
  TapeCopyToPoolMap copyToPoolMap;

  /**
   * The mount policy.
   */
  MountPolicy mountPolicy;
};

} // namespace cta::common::dataStructures
