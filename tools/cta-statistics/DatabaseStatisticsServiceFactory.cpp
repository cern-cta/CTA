/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DatabaseStatisticsServiceFactory.hpp"

namespace cta::statistics {

std::unique_ptr<DatabaseStatisticsService>
DatabaseStatisticsServiceFactory::create(cta::rdbms::Conn* databaseConnection, cta::rdbms::Login::DbType dbType) {
  using DbType = cta::rdbms::Login::DbType;
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

}  // namespace cta::statistics
