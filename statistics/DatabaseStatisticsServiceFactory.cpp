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


#include "DatabaseStatisticsServiceFactory.hpp"

namespace cta {
namespace statistics {

std::unique_ptr<DatabaseStatisticsService> DatabaseStatisticsServiceFactory::create(cta::rdbms::Conn* databaseConnection,
  cta::rdbms::Login::DbType dbType) {
  typedef cta::rdbms::Login::DbType DbType;
  std::unique_ptr<DatabaseStatisticsService> ret;
  switch (dbType) {
    case DbType::DBTYPE_IN_MEMORY:
    case DbType::DBTYPE_SQLITE:
    case DbType::DBTYPE_ORACLE:
    case DbType::DBTYPE_POSTGRESQL:
      ret.reset(new DatabaseStatisticsService(databaseConnection));
      return ret;
    default:
      throw cta::exception::Exception("In DatabaseStatisticsServiceFactory::create(), unknown database type.");
  }
}

}  // namespace statistics
}  // namespace cta
