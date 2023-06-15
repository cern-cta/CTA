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

#include <memory>

#include "rdbms/Conn.hpp"
#include "rdbms/Login.hpp"
#include "Statistics.hpp"
#include "StatisticsService.hpp"

namespace cta {
namespace statistics {

/**
 * Database statistics service that perform statistics operations
 * with a database backend
 * This class can be extended in order to execute database-specific SQL queries if needed
 * @param databaseConnection the database connection used by the service to perform statistics operations
 */
class DatabaseStatisticsService : public StatisticsService {
public:
  /**
   * Constructor
   * @param databaseConnection the database connection that will be used by this service
   */
  explicit DatabaseStatisticsService(cta::rdbms::Conn* databaseConnection);
  DatabaseStatisticsService(const DatabaseStatisticsService& orig) = delete;
  DatabaseStatisticsService& operator=(const DatabaseStatisticsService&) = delete;
  ~DatabaseStatisticsService() override;

  /**
   * Update the per-Tape statistics in the database used by this service
   */
  void updateStatisticsPerTape() override;

  /**
   * Saves the statistics in the service database
   * @param statistics the statistics to save in the database used by this service
   */
  void saveStatistics(const cta::statistics::Statistics& statistics) override;

  /**
   * Returns the statistics from the service database
   * @return the Statistics object that will contain the statistics retrieved from the database used by this service
   */
  std::unique_ptr<cta::statistics::Statistics> getStatistics() override;

protected:
  /**
   * The database connection of the database that will be used by the service
   */
  cta::rdbms::Conn& m_conn;

  /**
   * Saves the total file statistics in the database used by this service
   * @param statistics the statistics to save
   */
  // virtual void saveFileStatistics(const cta::statistics::Statistics & statistics);

  /**
   * Saves the per-VO statistics in the database used by this service
   * @param statistics the statistics to save
   */
  // virtual void saveStatisticsPerVo(const cta::statistics::Statistics & statistics);
};

}  // namespace statistics
}  // namespace cta
