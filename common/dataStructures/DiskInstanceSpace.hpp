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
 
#include <string>
#include "EntryLog.hpp"

namespace cta { 
namespace common {
namespace dataStructures {

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
   * The current free space associated to the disk system
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

}}} //namespace cta::common::dataStructures