/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "StatisticsServiceFactory.hpp"

#include "DatabaseStatisticsServiceFactory.hpp"
#include "JsonStatisticsServiceFactory.hpp"

namespace cta::statistics {

/**
 * Creates a database StatisticsService corresponding to the dbType passed in parameter
 * @param connection the connection to the database
 * @param dbType the databaseType of the database
 * @return the DatabaseStatisticsService corresponding to the dbType passed in parameter
 */
std::unique_ptr<StatisticsService> StatisticsServiceFactory::create(cta::rdbms::Conn* connection,
  cta::rdbms::Login::DbType dbType) {
  return DatabaseStatisticsServiceFactory::create(connection, dbType);
}

/**
 * Returns a JsonStatisticsService that will output in the ostream passed in parameter
 * @param ostream the stream where the json output will inserted
 * @return the JsonStatisticsService
 */
std::unique_ptr<StatisticsService> StatisticsServiceFactory::create(std::ostream& ostream) {
  return JsonStatisticsServiceFactory::create(&ostream);
}

} // namespace cta::statistics
