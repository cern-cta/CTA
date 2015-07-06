/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <stdint.h>
#include <string>

namespace cta {

/**
 * Class reprsenting the identity of a user.
 */
struct UserIdentity {

  /**
   * Constructor.
   *
   * Initialises all integer member-variables to non-valid values (max int).
   */
  UserIdentity() throw();

  /**
   * Constructor.
   *
   * @param uid The user ID of the user.
   * @param gid The group ID of the user.
   */
  UserIdentity(const uint32_t uid, const uint32_t gid) throw();

  /** 
   * Returns true if the specified right-hand side is equal to this object.
   *
   * @param rhs The object on the right-hand side of the == operator.
   * @return True if the specified right-hand side is equal to this object.
   */
  bool operator==(const UserIdentity &rhs) const;

  /**
   * Returns true if the specified right-hand side is not euqal to this object.
   *
   * @param rhs The object on the right-hand side of the != operator.
   * @return True if the specified right-hand side is not equal to this object.
   */
  bool operator!=(const UserIdentity &rhs) const;

  /**
   * The user ID of the user.
   */
  uint32_t uid;

  /**
   * The group ID of the user.
   */
  uint32_t gid;


}; // class UserIdentity

} // namespace cta

/**
 * Output stream operator for the cta::UserIdentity class.
 */
std::ostream &operator<<(std::ostream &os, const cta::UserIdentity &obj);
