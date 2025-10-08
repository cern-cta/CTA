/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/MountPolicy.hpp"

#include <map>
#include <stdint.h>

namespace cta::common::dataStructures {

/**
 * The queueing criteria returned by the catalogue for a file retrieve request
 */
struct RetrieveFileQueueCriteria {
  /**
   * The archived file
   */
  ArchiveFile archiveFile;

  /**
   * The mount policy
   */
  MountPolicy mountPolicy;
};

} // namespace cta::common::dataStructures
