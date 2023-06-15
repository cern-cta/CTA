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

#include <optional>
#include <string>

#include "EntryLog.hpp"

namespace cta {
namespace common {
namespace dataStructures {

struct VirtualOrganization {
  /**
   * The name
   */
  std::string name;
  /**
   * The comment.
   */
  std::string comment;
  /**
   * Maximum number of drives allocated for reading per VO
   */
  uint64_t readMaxDrives;
  /**
   * Max number of drives allocated for writing per VO
   */
  uint64_t writeMaxDrives;

  /**
   * Max size of files belonging to VO
   */
  uint64_t maxFileSize;

  /**
   * The creation log.
   */
  EntryLog creationLog;

  /**
   * The last modification log.
   */
  EntryLog lastModificationLog;

  /**
   * The disk instance name.
   */
  std::string diskInstanceName;

  bool operator==(const VirtualOrganization& other) const {
    return (name == other.name && comment == other.comment && readMaxDrives == other.readMaxDrives &&
            writeMaxDrives == other.writeMaxDrives && maxFileSize == other.maxFileSize &&
            diskInstanceName == other.diskInstanceName);
  }
};

}  // namespace dataStructures
}  // namespace common
}  // namespace cta
