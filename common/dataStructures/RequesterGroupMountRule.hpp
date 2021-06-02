/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
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

#include <list>
#include <map>
#include <stdint.h>
#include <string>

#include "common/dataStructures/EntryLog.hpp"

namespace cta {
namespace common {
namespace dataStructures {

/**
 * Rule specifying which mount policy will be used for a specific group of
 * data-transfer requesters.
 */
struct RequesterGroupMountRule {

  RequesterGroupMountRule();

  bool operator==(const RequesterGroupMountRule &rhs) const;

  bool operator!=(const RequesterGroupMountRule &rhs) const;

  /**
   * The name of the disk instance to which the requester group belongs.
   */
  std::string diskInstance;

  /**
   * The name of the requester group which is only guaranteed to be unique
   * within its disk instance.
   */
  std::string name;

  std::string mountPolicy;
  EntryLog creationLog;
  EntryLog lastModificationLog;
  std::string comment;

}; // struct RequesterGroupMountRule

std::ostream &operator<<(std::ostream &os, const RequesterGroupMountRule &obj);

} // namespace dataStructures
} // namespace common
} // namespace cta
