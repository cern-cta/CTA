/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#include "common/exception/Exception.hpp"
#include "common/exception/Errnum.hpp"
#include "common/exception/LostDatabaseConnection.hpp"
#include "common/make_unique.hpp"
#include "common/threading/MutexLocker.hpp"
#include "common/threading/RWLockRdLocker.hpp"
#include "common/threading/RWLockWrLocker.hpp"
#include "common/utils/utils.hpp"
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
  m_occiConn(conn),
  m_autocommitMode(AutocommitMode::AUTOCOMMIT_ON) {
  if(nullptr == conn) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed"
      ": The OCCI connection is a nullptr pointer");
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
OcciConn::~OcciConn() {
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
// setAutocommitMode
//------------------------------------------------------------------------------
void OcciConn::setAutocommitMode(const AutocommitMode autocommitMode) {
  threading::RWLockWrLocker wrLocker(m_autocommitModeRWLock);
  m_autocommitMode = autocommitMode;
}

//------------------------------------------------------------------------------
// getAutocommitMode
//------------------------------------------------------------------------------
AutocommitMode OcciConn::getAutocommitMode() const noexcept{
  threading::RWLockRdLocker rdLocker(m_autocommitModeRWLock);
  return m_autocommitMode;
}

//------------------------------------------------------------------------------
// executeNonQuery
//------------------------------------------------------------------------------
void OcciConn::executeNonQuery(const std::string &sql) {
  try {
    auto stmt = createStmt(sql);
    stmt->executeNonQuery();
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// createStmt
//------------------------------------------------------------------------------
std::unique_ptr<StmtWrapper> OcciConn::createStmt(const std::string &sql) {
  try {
    threading::MutexLocker locker(m_mutex);

    if(nullptr == m_occiConn) {
       throw exception::Exception("Connection is closed");
    }

    oracle::occi::Statement *const stmt = m_occiConn->createStatement(sql);
    if (nullptr == stmt) {
      throw exception::Exception("oracle::occi::createStatement() returned a nullptr pointer");
    }
    return cta::make_unique<OcciStmt>(sql, *this, stmt);
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
// getColumns
//------------------------------------------------------------------------------
std::map<std::string, std::string> OcciConn::getColumns(const std::string &tableName) {
  try {
    std::map<std::string, std::string> columnNamesAndTypes;
    const char *const sql =
      "SELECT "
        "COLUMN_NAME, "
        "DATA_TYPE "
      "FROM "
        "USER_TAB_COLUMNS "
      "WHERE "
        "TABLE_NAME = :TABLE_NAME";

    auto stmt = createStmt(sql);
    stmt->bindString(":TABLE_NAME", tableName);
    auto rset = stmt->executeQuery();
    while (rset->next()) {
      auto name = rset->columnOptionalString("COLUMN_NAME");
      auto type = rset->columnOptionalString("DATA_TYPE");
      if(name && type) {
         if ("NUMBER" == type.value()) {
           type = "NUMERIC";
         }
        columnNamesAndTypes.insert(std::make_pair(name.value(), type.value()));
      }
    }
    return columnNamesAndTypes;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = createStmt(sql);
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
// getIndexNames
//------------------------------------------------------------------------------
std::list<std::string> OcciConn::getIndexNames() {
  try {
    std::list<std::string> names;
    const char *const sql =
      "SELECT "
        "INDEX_NAME "
      "FROM "
        "USER_INDEXES "
      "ORDER BY "
        "INDEX_NAME";
    auto stmt = createStmt(sql);
    auto rset = stmt->executeQuery();
    while (rset->next()) {
      auto name = rset->columnOptionalString("INDEX_NAME");
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
    auto stmt = createStmt(sql);
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
// getTriggerNames
//------------------------------------------------------------------------------
std::list<std::string> OcciConn::getTriggerNames() {
  return std::list<std::string>();
}

//------------------------------------------------------------------------------
// getParallelTableNames
//------------------------------------------------------------------------------
std::list<std::string> OcciConn::getParallelTableNames() {
  try {
    std::list<std::string> names;
    const char *const sql =
      "SELECT "
        "TABLE_NAME "
      "FROM "
        "USER_TABLES "
      "WHERE "
        "TRIM(DEGREE) NOT LIKE '1' "
      "ORDER BY "
        "TABLE_NAME";
    auto stmt = createStmt(sql);
    auto rset = stmt->executeQuery();
    while (rset->next()) {
      auto name = rset->columnOptionalString("TABLE_NAME");
      names.push_back(name.value());
    }

    return names;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getConstraintNames
//------------------------------------------------------------------------------
std::list<std::string> OcciConn::getConstraintNames(const std::string& tableName){
  try {
    std::list<std::string> names;
    const char *const sql =
      "SELECT "
        "CONSTRAINT_NAME "
      "FROM "
        "USER_CONSTRAINTS "
      "WHERE "
        "TABLE_NAME=:TABLE_NAME";
    auto stmt = createStmt(sql);
    stmt->bindString(":TABLE_NAME",tableName);
    auto rset = stmt->executeQuery();
    while (rset->next()) {
      auto name = rset->columnOptionalString("CONSTRAINT_NAME");
      names.push_back(name.value());
    }
    return names;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getStoredProcedureNames
//------------------------------------------------------------------------------
std::list<std::string> OcciConn::getStoredProcedureNames() {
  try {
    std::list<std::string> names;
    const char *const sql =
      "SELECT "
        "OBJECT_NAME "
      "FROM "
        "USER_PROCEDURES";
    auto stmt = createStmt(sql);
    auto rset = stmt->executeQuery();
    while (rset->next()) {
      auto name = rset->columnOptionalString("OBJECT_NAME");
      names.push_back(name.value());
    }
    return names;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getSynonymNames
//------------------------------------------------------------------------------
std::list<std::string> OcciConn::getSynonymNames() {
  try {
    std::list<std::string> names;
    const char *const sql =
      "SELECT "
        "SYNONYM_NAME "
      "FROM "
        "USER_SYNONYMS";
    auto stmt = createStmt(sql);
    auto rset = stmt->executeQuery();
    while (rset->next()) {
      auto name = rset->columnOptionalString("SYNONYM_NAME");
      names.push_back(name.value());
    }
    return names;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getTypeNames
//------------------------------------------------------------------------------
std::list<std::string> OcciConn::getTypeNames() {
  try {
    std::list<std::string> names;
    const char *const sql =
      "SELECT "
        "TYPE_NAME "
      "FROM "
        "USER_TYPES";
    auto stmt = createStmt(sql);
    auto rset = stmt->executeQuery();
    while (rset->next()) {
      auto name = rset->columnOptionalString("TYPE_NAME");
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
