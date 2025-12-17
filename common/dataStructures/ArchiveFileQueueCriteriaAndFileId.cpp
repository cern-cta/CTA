/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/ArchiveFileQueueCriteriaAndFileId.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::common::dataStructures::ArchiveFileQueueCriteriaAndFileId::ArchiveFileQueueCriteriaAndFileId() : fileId(0) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::common::dataStructures::ArchiveFileQueueCriteriaAndFileId::ArchiveFileQueueCriteriaAndFileId(
  const uint64_t fileId,
  const TapeCopyToPoolMap& copyToPoolMap,
  const MountPolicy& mountPolicy)
    : fileId(fileId),
      copyToPoolMap(copyToPoolMap),
      mountPolicy(mountPolicy) {}
