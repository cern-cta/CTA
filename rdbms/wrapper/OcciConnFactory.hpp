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

#include "rdbms/wrapper/ConnFactory.hpp"

namespace cta {
namespace rdbms {
namespace wrapper {

/**
 * A concrete factory of Conn objects.
 */
class OcciConnFactory: public ConnFactory {
public:

  /**
   * Constructor.
   *
   * @param username The database username.
   * @param password The database password.
   * @param database The database name.
   */
  OcciConnFactory(
    const std::string &username,
    const std::string &password,
    const std::string &database);

  /**
   * Destructor.
   */
  ~OcciConnFactory() override;

  /**
   * Returns a newly created database connection.
   *
   * @return A newly created database connection.
   */
  std::unique_ptr<ConnWrapper> create() override;

private:

  /**
   * The database username.
   */
  std::string m_username;

  /**
   * The database password.
   */
  std::string m_password;

  /**
   * The database name.
   */
  std::string m_database;

}; // class OcciConnFactory

} // namespace wrapper
} // namespace rdbms
} // namespace cta
