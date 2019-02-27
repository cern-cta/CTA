/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2018  CERN
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

#include <vector>

namespace cta {
namespace rdbms {
namespace wrapper {

/**
 * A concrete factory of Conn objects.
 */
class PostgresConnFactory: public ConnFactory {
public:

  /**
   * Constructor.
   *
   * @param conninfo The conninfo string to pass to PQconnectdb. This is a series of key=value pairs separated by white space.
   *
   */
  PostgresConnFactory(const std::string& conninfo);

  /**
   * Destructor.
   */
  ~PostgresConnFactory() override;

  /**
   * Returns a newly created database connection.
   *
   * @return A newly created database connection.
   */
  std::unique_ptr<ConnWrapper> create() override;

private:

  /**
   * The conninfo string
   */
  std::string m_conninfo;

}; // class PostgresConnFactory

} // namespace wrapper
} // namespace rdbms
} // namespace cta
