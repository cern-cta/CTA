/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "Statistics.hpp"
#include "StatisticsService.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/Login.hpp"

#include <memory>

namespace cta::statistics {

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

  ~DatabaseStatisticsService() override = default;
  DatabaseStatisticsService(const DatabaseStatisticsService& orig) = delete;
  DatabaseStatisticsService& operator=(const DatabaseStatisticsService&) = delete;

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
};

}  // namespace cta::statistics
