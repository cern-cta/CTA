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

#include "StatisticsServiceFactory.hpp"

#include "DatabaseStatisticsServiceFactory.hpp"
#include "JsonStatisticsServiceFactory.hpp"

namespace cta {
namespace statistics {

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

}  // namespace statistics
}  // namespace cta
