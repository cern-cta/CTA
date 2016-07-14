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

#include <memory>

namespace cta {
namespace rdbms {

class DbConn;

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
  ~OcciEnv() throw();

  /**
   * Creates an OCCI connection.
   *
   * @param username The name of the database user.
   * @param password The database password.
   * @param database The name of the database.
   * @return The newly created OCCI connection.
   */
  DbConn *createConn(
    const std::string &username,
    const std::string &password,
    const std::string &database);

private:

  class Impl;

  /**
   * Pointer to the implementation (Pimple idiom).
   *
   * The Pimple idiom is being used to facilitate building CTA without OCCI support enabled.
   */
  std::unique_ptr<Impl> m_impl;

}; // class OcciEnv

} // namespace rdbms
} // namespace cta
