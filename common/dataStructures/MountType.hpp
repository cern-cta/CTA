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

namespace cta {
namespace common {
namespace dataStructures {

enum class MountType: uint32_t {
  ArchiveForUser = 1,
  ArchiveForRepack = 2,
  Retrieve = 3,
  Label = 4,
  NoMount = 0,
  /// A summary type used in scheduling.
  ArchiveAllTypes = 99
};

/**
 * A function summarizing subtypes (currently only Archive) to simplify scheduling.
 */
MountType getMountBasicType(MountType type);

/**
 * Convert enum to string for storage in DB and logging
 */
std::string toString(MountType type);

/**
 * Convert enum to string in camel case format to show in cta-admnin
 */
std::string toCamelCaseString(cta::common::dataStructures::MountType type);

/**
 * Convert string to enum. Needed to get values from DB.
 */
MountType strToMountType(const std::string& mountTypeStr);

std::ostream &operator <<(std::ostream& os, const MountType &obj);

} // namespace dataStructures
} // namespace common
} // namespace cta
