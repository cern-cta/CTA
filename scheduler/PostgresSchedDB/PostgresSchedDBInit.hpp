/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include "PostgresSchedDB.hpp"
#include "common/log/Logger.hpp"
#include "catalogue/Catalogue.hpp"

#include <memory>
#include <string>

namespace cta {

class PostgresSchedDBGC {
public:

  PostgresSchedDBGC(void *pgstuff, catalogue::Catalogue& catalogue) { }
  void runOnePass(log::LogContext & lc) { }
};

class PostgresSchedDBInit
{
public:
  PostgresSchedDBInit(const std::string& client_process, const std::string& db_conn_str, log::Logger& log,
    bool leaveNonEmptyAgentsBehind = false)
  {
  }

  std::unique_ptr<PostgresSchedDB> getSchedDB(catalogue::Catalogue& catalogue, log::Logger& log) {
    return std::make_unique<PostgresSchedDB>(nullptr, catalogue, log);
  }

  PostgresSchedDBGC getGarbageCollector(catalogue::Catalogue& catalogue) {
    return PostgresSchedDBGC(nullptr, catalogue);
  }

private:
};

typedef PostgresSchedDBInit      SchedulerDBInit_t;
typedef PostgresSchedDB          SchedulerDB_t;

} // namespace cta
