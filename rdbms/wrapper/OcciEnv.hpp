/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#include "rdbms/wrapper/ConnWrapper.hpp"

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
  std::unique_ptr<ConnWrapper> createConn(
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
