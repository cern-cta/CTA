/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
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

#include "common/exception/Exception.hpp"
#include "DbConnPool.hpp"
#include "OcciConnFactory.hpp"
#include "SqliteConnFactory.hpp"

#include <memory>

namespace cta {
namespace rdbms {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
DbConnPool::DbConnPool(const DbLogin &dbLogin, const uint64_t nbDbConns):
  m_dbLogin(dbLogin),
  m_nbDbConns(nbDbConns) {
  try {
    createDbConnFactory(dbLogin);
    createDbConns(m_nbDbConns);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// createDbConnFactory
//------------------------------------------------------------------------------
void DbConnPool::createDbConnFactory(const DbLogin &dbLogin) {
  try {
    switch(dbLogin.dbType) {
    case rdbms::DbLogin::DBTYPE_IN_MEMORY:
      m_dbConnFactory.reset(new SqliteConnFactory(":memory:"));
      break;
    case rdbms::DbLogin::DBTYPE_ORACLE:
      m_dbConnFactory.reset(new OcciConnFactory(dbLogin.username, dbLogin.password, dbLogin.database));
      break;
    case rdbms::DbLogin::DBTYPE_SQLITE:
      m_dbConnFactory.reset(new SqliteConnFactory(dbLogin.database));
      break;
    default:
      {
        exception::Exception ex;
        ex.getMessage() << "Unknown database type: value=" << dbLogin.dbType;
        throw ex;
      }
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// createDbConns
//------------------------------------------------------------------------------
void DbConnPool::createDbConns(const uint64_t nbDbConns) {
  try {
    for(uint64_t i = 0; i < nbDbConns; i++) {
      m_dbConns.push_back(m_dbConnFactory->create());
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
DbConnPool::~DbConnPool() throw() {
  for(auto &dbConn: m_dbConns) {
    delete dbConn;
  }
}

//------------------------------------------------------------------------------
// getDbConn
//------------------------------------------------------------------------------
DbConn *DbConnPool::getDbConn() {
  std::unique_lock<std::mutex> lock(m_dbConnsMutex);
  while(m_dbConns.size() == 0) {
    m_dbConnsCv.wait(lock);
  }

  DbConn *const dbConn = m_dbConns.front();
  m_dbConns.pop_front();
  return dbConn;
}

//------------------------------------------------------------------------------
// returnDbConn
//------------------------------------------------------------------------------
void DbConnPool::returnDbConn(DbConn *const dbConn) {
  std::unique_lock<std::mutex> lock(m_dbConnsMutex);
  m_dbConns.push_back(dbConn);
  m_dbConnsCv.notify_one();
}

} // namespace rdbms
} // namespace cta
