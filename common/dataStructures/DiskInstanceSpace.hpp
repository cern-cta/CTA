/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>
#include "EntryLog.hpp"

namespace cta::common::dataStructures {

struct DiskInstanceSpace {

   /**
   * The name
   */
  std::string name;

   /**
   * The disk instance
   */
  std::string diskInstance;

  /**
   * The URL to query to obtain the free space.
   */
  std::string freeSpaceQueryURL;

  /**
   * The free space query period
   */
  uint64_t refreshInterval;

  /**
   * The current free space associated to the disk instance space
   */
  uint64_t freeSpace;

  /**
   * The timestamp of the last free space query
   */
  uint64_t lastRefreshTime;

  /**
   * The comment.
   */
  std::string comment;

  /**
   * The creation log.
   */
  EntryLog creationLog;

  /**
   * The last modification log.
   */
  EntryLog lastModificationLog;

  bool operator==(const DiskInstanceSpace & other) const{
    return (name == other.name && diskInstance == other.diskInstance && comment == other.comment);
  }
};

} // namespace cta::common::dataStructures
