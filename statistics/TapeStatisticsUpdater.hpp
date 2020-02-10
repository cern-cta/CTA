/**
 * The CERN Tape Archive (CTA) project
 * Copyright © 2018 CERN
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

namespace cta { namespace statistics {

class TapeStatisticsUpdater {
public:
  TapeStatisticsUpdater(cta::rdbms::Conn &conn);
  virtual void updateTapeStatistics();
  virtual uint64_t getNbUpdatedTapes();
  virtual ~TapeStatisticsUpdater();
protected:
  cta::rdbms::Conn &m_conn;
  uint64_t m_nbUpdatedTapes = 0;
};

//TODO if necessary
class MySQLStatisticsUpdater: public TapeStatisticsUpdater {
public:
  MySQLStatisticsUpdater(cta::rdbms::Conn &conn);
  virtual ~MySQLStatisticsUpdater();
  void updateTapeStatistics() override;
};

class TapeStatisticsUpdaterFactory {
public:
  static std::unique_ptr<TapeStatisticsUpdater> create(cta::rdbms::Login::DbType dbType, cta::rdbms::Conn &conn);
};

}}