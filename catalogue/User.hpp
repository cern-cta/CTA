/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <compare>
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
   * Comparison operators.
   *
   * These are defaulted so that they compare the members lexicographically, in
   * declaration order. Do not hand-write them: this type is used as the key of
   * the std::map behind the requester mount policy cache, so an ordering that is
   * not a strict weak ordering is undefined behaviour.
   */
  auto operator<=>(const User& rhs) const = default;
};  // struct User

}  // namespace cta::catalogue
