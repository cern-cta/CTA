/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "rdbms/wrapper/ConnFactory.hpp"

namespace cta {
namespace rdbms {
namespace wrapper {

/**
 * A concrete factory of Conn objects.
 */
class MysqlConnFactory: public ConnFactory {
public:

  /**
   * Constructor.
   *
   * @param host The host name.
   * @param user The user name.
   * @param passwd The password.
   * @param db The database name.
   * @param port The port number.
   *
   */
  MysqlConnFactory(const std::string& host, 
                   const std::string& user, 
                   const std::string& passwd,
                   const std::string& db,
                   unsigned int port);

  /**
   * Destructor.
   */
  ~MysqlConnFactory() override;

  /**
   * Returns a newly created database connection.
   *
   * @return A newly created database connection.
   */
  std::unique_ptr<ConnWrapper> create() override;

private:

  /**
   * The host name, user name, password, database name and port number
   */
  std::string m_host;
  std::string m_user;
  std::string m_passwd;
  std::string m_db;
  unsigned int m_port;

}; // class MysqlConnFactory

} // namespace wrapper
} // namespace rdbms
} // namespace cta
