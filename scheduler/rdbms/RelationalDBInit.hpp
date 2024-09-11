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

#include <memory>
#include <string>

#include "scheduler/rdbms/RelationalDB.hpp"
#include "common/log/Logger.hpp"

namespace cta::catalogue {

class Catalogue;
}

namespace cta {

class RelationalDBQCR {
// QueueCleanupRunner
public:

  RelationalDBQCR(catalogue::Catalogue &catalogue, RelationalDB &pgs) { }
  void runOnePass(log::LogContext & lc) {
    // get the current time stamps in epoch

    // create new partitions if needed

    // check and drop partitions which are ready to be dropped
  }
};

class RelationalDBGC {
// GarbageCollector
public:

  RelationalDBGC(void *pgstuff, catalogue::Catalogue& catalogue) { }
  void runOnePass(log::LogContext & lc) { }
};

class RelationalDBInit
{
public:
  RelationalDBInit(const std::string& client_process, const std::string& db_conn_str, log::Logger& log,
    bool leaveNonEmptyAgentsBehind = false)
  {
    connStr = db_conn_str;
    clientProc = client_process;
    login = rdbms::Login::parseString(connStr);
    if (login.dbType != rdbms::Login::DBTYPE_POSTGRESQL) {
      std::runtime_error("scheduler dbconnect string must specify postgres.");
    }
  }

  std::unique_ptr<RelationalDB> getSchedDB(catalogue::Catalogue& catalogue, log::Logger& log) {
    const uint64_t nbConns = 10;
    return std::make_unique<RelationalDB>( clientProc, log, catalogue, login, nbConns);
  }

  RelationalDBGC getGarbageCollector(catalogue::Catalogue& catalogue) {
    return RelationalDBGC(nullptr, catalogue);
  }

  RelationalDBQCR getQueueCleanupRunner(catalogue::Catalogue& catalogue, RelationalDB &pgs) {
    return RelationalDBQCR(catalogue, pgs);
  }

private:
  std::string connStr;
  std::string clientProc;
  rdbms::Login login;
};

typedef RelationalDBInit      SchedulerDBInit_t;
typedef RelationalDB          SchedulerDB_t;

} // namespace cta