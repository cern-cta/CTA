/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <stdint.h>

#include <list>
#include <map>
#include <string>

#include "common/dataStructures/EntryLog.hpp"

namespace cta {
namespace common {
namespace dataStructures {

/**
 * Specifies the minimum criteria needed to warrant a mount
 */
struct MountPolicy {
  MountPolicy();

  bool operator==(const MountPolicy &rhs) const;

  bool operator!=(const MountPolicy &rhs) const;

  MountPolicy operator=(const MountPolicy& other);

  std::string name;
  uint64_t archivePriority;
  uint64_t archiveMinRequestAge;
  uint64_t retrievePriority;
  uint64_t retrieveMinRequestAge;
  EntryLog creationLog;
  EntryLog lastModificationLog;
  std::string comment;

  // As repack request does not have mount policy yet, we need to create a fake one to be able
  // to do a Retrieve mount or Archive mount
  static struct MountPolicy s_defaultMountPolicyForRepack;

 private:
  MountPolicy(const std::string& name, const uint64_t archivePriority, const uint64_t archiveMinRequestAge,
    const uint64_t retrievePriority, const uint64_t retrieveMinRequestAge);
};  // struct MountPolicy

std::ostream &operator<<(std::ostream &os, const MountPolicy &obj);

}  // namespace dataStructures
}  // namespace common
}  // namespace cta
