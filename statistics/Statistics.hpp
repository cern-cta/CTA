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
#include <map>
#include "FileStatistics.hpp"
#include "rdbms/Rset.hpp"

namespace cta { namespace statistics {
    
/**
 * This class will be used by the StatistcsGetter to
 * fill the statistics into it.
 * It will be also used by the StatisticsSaver to save the statistics
 * into the Statistics database
 * It is composed of a map[VirtualOrganization][FileStatistics]
 */    
class Statistics {
public:
  typedef std::map<std::string,FileStatistics> StatisticsPerVo;
  
  Statistics();
  Statistics(const Statistics &other);
  Statistics &operator=(const Statistics &other);
  void insertStatistics(const std::string &vo, const FileStatistics &fileStatistics);
  const StatisticsPerVo &getAllStatistics() const;
  uint64_t getTotalFiles() const;
  uint64_t getTotalBytes() const;
  
  /**
   * This builder class allows to build the statistics
   * with the ResultSet returned from the statistics database query
   * @param rset the results of the statistics query
   */
  class Builder {
  public:
    Builder(cta::rdbms::Rset & rset);
    std::unique_ptr<Statistics> build();
  private:
    cta::rdbms::Rset &m_rset;
    std::unique_ptr<Statistics> m_statistics;
  };
  
private:
  
  StatisticsPerVo m_statisticsPerVo;
  uint64_t m_totalFiles = 0;
  uint64_t m_totalBytes = 0;
};

std::ostream &operator <<(std::ostream &stream, Statistics stats);

}}

