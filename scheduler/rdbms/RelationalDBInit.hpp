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

#include "common/Timer.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/Logger.hpp"
#include "scheduler/rdbms/RelationalDB.hpp"
#include "scheduler/rdbms/postgres/Enums.hpp"

#include <memory>
#include <string>

namespace cta {

class RelationalDBInit {
public:
  RelationalDBInit(const std::string& client_process,
                   const std::string& db_conn_str,
                   log::Logger& log,
                   bool leaveNonEmptyAgentsBehind = false) {
    connStr = db_conn_str;
    clientProc = client_process;
    login = rdbms::Login::parseString(connStr);
    if (login.dbType != rdbms::Login::DBTYPE_POSTGRESQL) {
      std::runtime_error("scheduler dbconnect string must specify postgres.");
    }
  }

  std::unique_ptr<RelationalDB> getSchedDB(catalogue::Catalogue& catalogue, log::Logger& log) {
    const uint64_t nbConns = 20;
    return std::make_unique<RelationalDB>(clientProc, log, catalogue, login, nbConns);
  }

private:
  std::string connStr;
  std::string clientProc;
  rdbms::Login login;
};

using SchedulerDBInit_t = RelationalDBInit;
using SchedulerDB_t = RelationalDB;

}  // namespace cta
