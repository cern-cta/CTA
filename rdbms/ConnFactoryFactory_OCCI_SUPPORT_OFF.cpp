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

#include "common/exception/Exception.hpp"
#include "common/make_unique.hpp"
#include "rdbms/ConnFactoryFactory.hpp"
#include "rdbms/SqliteConnFactory.hpp"

namespace cta {
namespace rdbms {

//------------------------------------------------------------------------------
// create
//------------------------------------------------------------------------------
std::unique_ptr<ConnFactory> ConnFactoryFactory::create(const Login &login) {
  try {
    switch(login.dbType) {
    case rdbms::Login::DBTYPE_IN_MEMORY:
      return cta::make_unique<SqliteConnFactory>(":memory:");
    case rdbms::Login::DBTYPE_ORACLE:
      throw exception::Exception("OCCI support disabled at compile time");
    case rdbms::Login::DBTYPE_SQLITE:
      return cta::make_unique<SqliteConnFactory>(login.database);
    case rdbms::Login::DBTYPE_NONE:
      throw exception::Exception("Cannot create a catalogue without a database type");
    default:
      {
        exception::Exception ex;
        ex.getMessage() << "Unknown database type: value=" << login.dbType;
        throw ex;
      }
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

} // namespace rdbms
} // namespace cta
