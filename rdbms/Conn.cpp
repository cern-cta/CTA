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
#include "common/utils/utils.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/rdbms.hpp"

namespace cta {
namespace rdbms {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Conn::Conn() : m_pool(nullptr) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Conn::Conn(std::unique_ptr<ConnAndStmts> connAndStmts, ConnPool* pool) :
m_connAndStmts(std::move(connAndStmts)),
m_pool(pool) {}

//------------------------------------------------------------------------------
// move constructor
//------------------------------------------------------------------------------
Conn::Conn(Conn&& other) : m_connAndStmts(std::move(other.m_connAndStmts)), m_pool(other.m_pool) {
  other.m_pool = nullptr;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
Conn::~Conn() noexcept {
  reset();
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void Conn::reset() noexcept {
  try {
    // If this smart database connection currently points to a database connection then return it back to its pool
    if (nullptr != m_pool && nullptr != m_connAndStmts) {
      m_pool->returnConn(std::move(m_connAndStmts));
    }
  }
  catch (...) {
  }

  m_pool = nullptr;
  m_connAndStmts.reset();
}

//------------------------------------------------------------------------------
// operator=
//------------------------------------------------------------------------------
Conn& Conn::operator=(Conn&& rhs) {
  // If the database connection is not the one already owned
  if (rhs.m_connAndStmts != m_connAndStmts) {
    // If this smart database connection currently points to a database connection then return it back to its pool
    if (nullptr != m_pool && nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
      m_pool->returnConn(std::move(m_connAndStmts));
    }

    // Take ownership of the new database connection
    m_connAndStmts = std::move(rhs.m_connAndStmts);
    m_pool = rhs.m_pool;

    rhs.m_pool = nullptr;
  }
  return *this;
}

//------------------------------------------------------------------------------
// setAutocommitMode
//------------------------------------------------------------------------------
void Conn::setAutocommitMode(const AutocommitMode autocommitMode) {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    m_connAndStmts->conn->setAutocommitMode(autocommitMode);
  }
  else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// getAutocommitMode
//------------------------------------------------------------------------------
AutocommitMode Conn::getAutocommitMode() const {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getAutocommitMode();
  }
  else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// createStmt
//------------------------------------------------------------------------------
Stmt Conn::createStmt(const std::string& sql) {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->stmtPool->getStmt(*m_connAndStmts->conn, sql);
  }
  else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// executeNonQuery
//------------------------------------------------------------------------------
void Conn::executeNonQuery(const std::string& sql) {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    //  if(std::string::npos != sql.find(";")) {
    //    UnexpectedSemicolon ex;
    //    ex.getMessage() << "Encountered unexpected semicolon in " << getSqlForException(sql);
    //    throw ex;
    //  }
    m_connAndStmts->conn->executeNonQuery(sql);
  }
  else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// commit
//------------------------------------------------------------------------------
void Conn::commit() {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    m_connAndStmts->conn->commit();
  }
  else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// commit
//------------------------------------------------------------------------------
void Conn::rollback() {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    m_connAndStmts->conn->rollback();
  }
  else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// getSchemaColumns
//------------------------------------------------------------------------------
std::map<std::string, std::string> Conn::getColumns(const std::string& tableName) const {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getColumns(tableName);
  }
  else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// getTableNames
//------------------------------------------------------------------------------
std::list<std::string> Conn::getTableNames() const {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getTableNames();
  }
  else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// getIndexNames
//------------------------------------------------------------------------------
std::list<std::string> Conn::getIndexNames() const {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getIndexNames();
  }
  else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// closeUnderlyingStmtsAndConn
//------------------------------------------------------------------------------
void Conn::closeUnderlyingStmtsAndConn() {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    m_connAndStmts->stmtPool->clear();
    m_connAndStmts->conn->close();
  }
  else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// isOpen
//------------------------------------------------------------------------------
bool Conn::isOpen() const {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->isOpen();
  }
  else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// getSequenceNames
//------------------------------------------------------------------------------
std::list<std::string> Conn::getSequenceNames() {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getSequenceNames();
  }
  else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// getTriggerNames
//------------------------------------------------------------------------------
std::list<std::string> Conn::getTriggerNames() {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getTriggerNames();
  }
  else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// getParallelTableNames
//------------------------------------------------------------------------------
std::list<std::string> Conn::getParallelTableNames() {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getParallelTableNames();
  }
  else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// getConstraintNames
//------------------------------------------------------------------------------
std::list<std::string> Conn::getConstraintNames(const std::string& tableName) {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getConstraintNames(tableName);
  }
  else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// getStoredProcedureNames
//------------------------------------------------------------------------------
std::list<std::string> Conn::getStoredProcedureNames() {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getStoredProcedureNames();
  }
  else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// getSynonymNames
//------------------------------------------------------------------------------
std::list<std::string> Conn::getSynonymNames() {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getSynonymNames();
  }
  else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// getTypeNames
//------------------------------------------------------------------------------
std::list<std::string> Conn::getTypeNames() {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getTypeNames();
  }
  else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

}  // namespace rdbms
}  // namespace cta
