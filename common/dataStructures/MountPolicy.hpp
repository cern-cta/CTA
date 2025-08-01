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

#include <stdint.h>
#include <list>
#include <map>
#include <string>

#include "common/dataStructures/EntryLog.hpp"

namespace cta::common::dataStructures {

/**
 * Specifies the minimum criteria needed to warrant a mount
 */
struct MountPolicy {
  MountPolicy() = default;

  MountPolicy(const MountPolicy& other) = default;
  MountPolicy& operator=(const MountPolicy& other) = default;

  bool operator==(const MountPolicy &rhs) const;
  bool operator!=(const MountPolicy &rhs) const;

  std::string name;
  uint64_t archivePriority = 0;
  uint64_t archiveMinRequestAge = 0;
  uint64_t retrievePriority = 0;
  uint64_t retrieveMinRequestAge = 0;
  EntryLog creationLog;
  EntryLog lastModificationLog;
  std::string comment;

  // As repack request does not have mount policy yet, we need to create a fake one to be able
  // to do a Retrieve mount or Archive mount
  static const MountPolicy s_defaultMountPolicyForRepack;

  MountPolicy(std::string_view mpName, uint64_t archivePriority, uint64_t archiveMinRequestAge,
    uint64_t retrievePriority, uint64_t retrieveMinRequestAge);
};

std::ostream& operator<<(std::ostream& os, const MountPolicy& obj);

} // namespace cta::common::dataStructures
