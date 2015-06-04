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
class UserIdentity {
public:

  /**
   * Constructor.
   *
   * Initialises all integer member-variables to 0.
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
   * Sets the user ID of the user.
   *
   * @param uid The user ID of the user.
   */
  void setUid(const uint32_t uid) throw();

  /**
   * Returns the user ID of the user.
   */
  uint32_t getUid() const throw();

  /**
   * Sets the group ID of the user.
   *
   * @param gid The group ID of the user.
   */
  void setGid(const uint32_t gid) throw();

  /**
   * Returns the group ID of the user.
   */
  uint32_t getGid() const throw();

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

private:

  /**
   * The user ID of the user.
   */
  uint32_t m_uid;

  /**
   * The group ID of the user.
   */
  uint32_t m_gid;


}; // class UserIdentity

} // namespace cta

/**
 * Output stream operator for the cta::UserIdentity class.
 */
std::ostream &operator<<(std::ostream &os, const cta::UserIdentity &obj);
