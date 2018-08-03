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

#include "rdbms/wrapper/Conn.hpp"

#include <memory>
#include <occi.h>

namespace cta {
namespace rdbms {
namespace wrapper {

/**
 * A convenience wrapper around an OCCI environment.
 */
class OcciEnv {
public:

  /**
   * Constructor.
   *
   * Creates an OCCI environment with a THREADED_MUTEXED mode.
   */
  OcciEnv();

  /**
   * Destructor.
   *
   * Terminates the underlying OCCI environment.
   */
  ~OcciEnv();

  /**
   * Creates an OCCI connection.
   *
   * This method will throw an exception if either the username, password ori
   * database parameters are nullptr pointers.
   *
   * @param username The name of the database user.
   * @param password The database password.
   * @param database The name of the database.
   * @return The newly created OCCI connection.
   */
  std::unique_ptr<Conn> createConn(
    const std::string &username,
    const std::string &password,
    const std::string &database);

private:

  /**
   * The OCCI environment.
   */
  oracle::occi::Environment *m_env;

}; // class OcciEnv

} // namespace wrapper
} // namespace rdbms
} // namespace cta
