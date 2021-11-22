/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
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

#include <string>

#include "common/exception/Exception.hpp"
#include "common/make_unique.hpp"
#include "rdbms/wrapper/ConnFactoryFactory.hpp"
#include "rdbms/wrapper/OcciConnFactory.hpp"
#include "rdbms/wrapper/SqliteConnFactory.hpp"
#include "rdbms/wrapper/PostgresConnFactory.hpp"

namespace cta {
namespace rdbms {
namespace wrapper {

//------------------------------------------------------------------------------
// create
//------------------------------------------------------------------------------
std::unique_ptr<ConnFactory> ConnFactoryFactory::create(const Login &login) {
  try {
    switch (login.dbType) {
    case Login::DBTYPE_IN_MEMORY:
      return cta::make_unique<SqliteConnFactory>("file::memory:?cache=shared");
    case Login::DBTYPE_ORACLE:
      return cta::make_unique<OcciConnFactory>(login.username, login.password, login.database);
    case Login::DBTYPE_SQLITE:
      return cta::make_unique<SqliteConnFactory>(login.database);
    case Login::DBTYPE_POSTGRESQL:
      return cta::make_unique<PostgresConnFactory>(login.database);
    case Login::DBTYPE_NONE:
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

}  // namespace wrapper
}  // namespace rdbms
}  // namespace cta
