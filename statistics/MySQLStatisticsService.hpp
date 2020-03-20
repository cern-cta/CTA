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
#include "DatabaseStatisticsService.hpp"

namespace cta { namespace statistics { 

  /**
   * Contains MySQL-specific queries
   * @param databaseConnection the MySQL database connection
   */
class MySQLStatisticsService: public DatabaseStatisticsService {
public:
  /**
   * Constructor
   * @param databaseConnection the MySQL database connection
   */
  MySQLStatisticsService(cta::rdbms::Conn & databaseConnection);
  MySQLStatisticsService(const MySQLStatisticsService& orig) = delete;
  MySQLStatisticsService & operator=(const MySQLStatisticsService &) = delete;
  virtual ~MySQLStatisticsService();
  
  /**
   * This method needs to be implemented for MySQL CTA catalogue
   * as the UPDATE query only works for PostgreSQL and Oracle
   */
  void updateStatisticsPerTape() override;
private:

};

}}
