/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>

namespace cta::catalogue {

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

} // namespace cta::catalogue
