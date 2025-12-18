/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
