/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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
namespace catalogue {

/**
  * A fully qualified user group, in other words the name of the disk instance
  * and the name of the group.
  */
struct Group {
  /**
   * The name of the disk instance to which the group name belongs.
   */
  std::string diskInstanceName;

  /**
   * The name of the group which is only guaranteed to be unique within its
   * disk instance.
   */
  std::string groupName;

  /**
   * Constructor.
   *
   * @param d The name of the disk instance to which the group name belongs.
   * @param g The name of the group which is only guaranteed to be unique
   * within its disk instance.
   */
  Group(const std::string &d, const std::string &g): diskInstanceName(d), groupName(g) {
  }

  /**
   * Less than operator.
   *
   * @param rhs The argument on the right hand side of the operator.
   * @return True if this object is less than the argument on the right hand
   * side of the operator.
   */
  bool operator<(const Group &rhs) const {
    return diskInstanceName < rhs.diskInstanceName || groupName < rhs.groupName;
  }
}; // struct Group

} // namespace catalogue
} // namespace cta
