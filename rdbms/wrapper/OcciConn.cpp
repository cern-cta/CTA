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
#include "common/make_unique.hpp"
#include "common/threading/MutexLocker.hpp"
#include "rdbms/wrapper/OcciConn.hpp"
#include "rdbms/wrapper/OcciEnv.hpp"
#include "rdbms/wrapper/OcciStmt.hpp"

#include <stdexcept>
#include <string>

namespace cta {
namespace rdbms {
namespace wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OcciConn::OcciConn(oracle::occi::Environment *const env, oracle::occi::Connection *const conn):
  m_env(env),
  m_occiConn(conn) {
  if(nullptr == conn) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed"
      ": The OCCI connection is a nullptr pointer");
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
OcciConn::~OcciConn() throw() {
  try {
    close(); // Idempotent close() method
  } catch(...) {
    // Destructor should not throw any exceptions
  }
}

//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void OcciConn::close() {
  threading::MutexLocker locker(m_mutex);

  if(nullptr != m_occiConn) {
    m_env->terminateConnection(m_occiConn);
    m_occiConn = nullptr;
  }
}

//------------------------------------------------------------------------------
// createStmt
//------------------------------------------------------------------------------
std::unique_ptr<Stmt> OcciConn::createStmt(const std::string &sql, AutocommitMode autocommitMode) {
  try {
    threading::MutexLocker locker(m_mutex);

    if(nullptr == m_occiConn) {
       throw exception::Exception("Connection is closed");
    }

    oracle::occi::Statement *const stmt = m_occiConn->createStatement(sql);
    if (nullptr == stmt) {
      throw exception::Exception("oracle::occi::createStatement() returned a nullptr pointer");
    }
    return cta::make_unique<OcciStmt>(autocommitMode, sql, *this, stmt);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + sql + ": " +
      ex.getMessage().str());
  } catch(std::exception &se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + sql + ": " + se.what());
  }
}

//------------------------------------------------------------------------------
// commit
//------------------------------------------------------------------------------
void OcciConn::commit() {
  try {
    threading::MutexLocker locker(m_mutex);

    if(nullptr == m_occiConn) {
       throw exception::Exception("Connection is closed");
    }

    m_occiConn->commit();
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  } catch(std::exception &se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + se.what());
  }
}

//------------------------------------------------------------------------------
// rollback
//------------------------------------------------------------------------------
void OcciConn::rollback() {
  try {
    threading::MutexLocker locker(m_mutex);

    if(nullptr == m_occiConn) {
       throw exception::Exception("Connection is closed");
    }

    m_occiConn->rollback();
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  } catch(std::exception &se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + se.what());
  }
}

//------------------------------------------------------------------------------
// getTableNames
//------------------------------------------------------------------------------
std::list<std::string> OcciConn::getTableNames() {
  try {
    std::list<std::string> names;
    const char *const sql =
      "SELECT "
        "TABLE_NAME "
      "FROM "
        "USER_TABLES "
      "ORDER BY "
        "TABLE_NAME";
    auto stmt = createStmt(sql, AutocommitMode::OFF);
    auto rset = stmt->executeQuery();
    while (rset->next()) {
      auto name = rset->columnOptionalString("TABLE_NAME");
      if(name) {
        names.push_back(name.value());
      }
    }

    return names;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getSequenceNames
//------------------------------------------------------------------------------
std::list<std::string> OcciConn::getSequenceNames() {
  try {
    std::list<std::string> names;
    const char *const sql =
      "SELECT "
        "SEQUENCE_NAME "
      "FROM "
        "USER_SEQUENCES "
      "ORDER BY "
        "SEQUENCE_NAME";
    auto stmt = createStmt(sql, AutocommitMode::OFF);
    auto rset = stmt->executeQuery();
    while (rset->next()) {
      auto name = rset->columnOptionalString("SEQUENCE_NAME");
      names.push_back(name.value());
    }

    return names;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// isOpen
//------------------------------------------------------------------------------
bool OcciConn::isOpen() const {
  return nullptr != m_occiConn;
}

//------------------------------------------------------------------------------
// closeStmt
//------------------------------------------------------------------------------
void OcciConn::closeStmt(oracle::occi::Statement *const stmt) {
  try {
    threading::MutexLocker locker(m_mutex);

    if(nullptr == m_occiConn) {
       throw exception::Exception("Connection is closed");
    }

    if(nullptr == stmt) {
      throw exception::Exception("stmt is a nullptr");
    }

    m_occiConn->terminateStatement(stmt);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  } catch(std::exception &se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + se.what());
  }
}

} // namespace wrapper
} // namespace rdbms
} // namespace cta
