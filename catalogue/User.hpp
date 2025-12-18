/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

namespace cta::catalogue {

/**
  * A fully qualified user, in other words the name of the disk instance and
  * the name of the group.
  */
struct User {
  /**
   * The name of the disk instance to which the user name belongs.
   */
  std::string diskInstanceName;

  /**
   * The name of the user which is only guaranteed to be unique within its
   * disk instance.
   */
  std::string username;

  /**
   * Constructor.
   *
   * @param d The name of the disk instance to which the group name belongs.
   * @param u The name of the group which is only guaranteed to be unique
   * within its disk instance.
   */
  User(const std::string& d, const std::string& u) : diskInstanceName(d), username(u) {}

  /**
   * Less than operator.
   *
   * @param rhs The argument on the right hand side of the operator.
   * @return True if this object is less than the argument on the right hand
   * side of the operator.
   */
  bool operator<(const User& rhs) const { return diskInstanceName < rhs.diskInstanceName || username < rhs.username; }
};  // struct User

}  // namespace cta::catalogue
