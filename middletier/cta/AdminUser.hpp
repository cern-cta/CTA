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

#include "cta/ConfigurationItem.hpp"
#include "cta/UserIdentity.hpp"

#include <string>

namespace cta {

/**
 * Class representing an administrator.
 */
class AdminUser: public ConfigurationItem {
public:

  /**
   * Constructor.
   */
  AdminUser();

  /**
   * Destructor.
   */
  ~AdminUser() throw();

  /**
   * Constructor.
   *
   * @param user The identity of the administrator.
   * @param creator The identity of the user that created this configuration
   * item.
   * @param comment The comment made by the creator of this configuration
   * item.
   * @param creationTime Optionally the absolute time at which this
   * configuration item was created.  If no value is given then the current
   * time is used.
   */
  AdminUser(
    const UserIdentity &user,
    const UserIdentity &creator,
    const std::string &comment,
    const time_t creationTime = time(NULL));

  /**
   * Returns the identity of the administrator.
   *
   * @return The identity of the administrator.
   */
  const UserIdentity &getUser() const throw();

private:

  /**
   * The identity of the administrator.
   */
  UserIdentity m_user;

}; // class AdminUser

} // namespace cta
