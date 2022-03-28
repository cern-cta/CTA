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
