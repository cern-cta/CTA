/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019  CERN
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


#pragma once
#include "rdbms/Conn.hpp"
#include "rdbms/Login.hpp"
#include "StatisticsService.hpp"

namespace cta { namespace statistics {
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
  static std::unique_ptr<StatisticsService> create(cta::rdbms::Conn &connection, cta::rdbms::Login::DbType dbType);

};

}}