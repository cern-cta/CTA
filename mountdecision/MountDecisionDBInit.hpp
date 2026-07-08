/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/Exception.hpp"
#include "common/log/Logger.hpp"
#include "mountdecision/MountDecisionDB.hpp"
#include "rdbms/Login.hpp"

#include <cstdint>
#include <memory>
#include <string>

namespace cta::mountdecision {

class MountDecisionDBInit {
public:
  MountDecisionDBInit(const std::string& clientProcess, const std::string& dbConnStr, uint16_t dbNbConns, log::Logger&)
      : m_connStr(dbConnStr),
        m_numberOfConnections(dbNbConns),
        m_clientProcess(clientProcess) {
    m_login = rdbms::Login::parseString(m_connStr);
    if (m_login.dbType != rdbms::Login::DBTYPE_POSTGRESQL) {
      throw exception::Exception("Mount Decision DB connection string must specify postgresql.");
    }
  }

  std::unique_ptr<MountDecisionDB> getDB(log::Logger& log) {
    log(log::INFO,
        "Setting number of connections for Mount Decision DB pool.",
        {cta::log::Param("numberOfConnections", m_numberOfConnections)});
    return std::make_unique<MountDecisionDB>(m_clientProcess, log, m_login, m_numberOfConnections);
  }

private:
  std::string m_connStr;
  uint16_t m_numberOfConnections;
  std::string m_clientProcess;
  rdbms::Login m_login;
};

}  // namespace cta::mountdecision
