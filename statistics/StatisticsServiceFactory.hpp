/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "StatisticsService.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/Login.hpp"

#include <memory>

namespace cta::statistics {

/**
 * Factory to instanciate a StatisticsService
 */
class StatisticsServiceFactory {
public:
  /**
   * Creates a StatisticsService to perform Statistics operations
   * @param connection the database connection
   * @param dbType the database type of the database connection
   * @return a unique_ptr containing the StatisticsService
   */
  static std::unique_ptr<StatisticsService> create(cta::rdbms::Conn* connection, cta::rdbms::Login::DbType dbType);

  /**
   * Creates a StatisticsService to perform Statistics operations
   * @return a unique_ptr containing the StatisticsService
   */
  static std::unique_ptr<StatisticsService> create(std::ostream& ostream);
};

}  // namespace cta::statistics
