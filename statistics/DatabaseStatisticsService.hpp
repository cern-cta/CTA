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
#include "Statistics.hpp"
#include "StatisticsService.hpp"

namespace cta { namespace statistics {
  
class DatabaseStatisticsService: public StatisticsService {
public:
  DatabaseStatisticsService(cta::rdbms::Conn &databaseConnection);
  DatabaseStatisticsService(const DatabaseStatisticsService& orig) = delete;
  DatabaseStatisticsService & operator=(const DatabaseStatisticsService &) = delete;
  virtual ~DatabaseStatisticsService();
  
  virtual void updateStatistics() override;
  virtual void saveStatistics(const cta::statistics::Statistics &statistics) override;
  virtual std::unique_ptr<cta::statistics::Statistics> getStatistics() override;
protected:
  cta::rdbms::Conn & m_conn;
  virtual void saveFileStatistics(const cta::statistics::Statistics & statistics);
  virtual void saveStatisticsPerVo(const cta::statistics::Statistics & statistics);
};

}}

