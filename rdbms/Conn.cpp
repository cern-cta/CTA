/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rdbms/Conn.hpp"

#include "common/exception/Exception.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/telemetry/metrics/instruments/RdbmsInstruments.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/rdbms.hpp"

#include <chrono>
#include <opentelemetry/context/runtime_context.h>

namespace cta::rdbms {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Conn::Conn() : m_pool(nullptr) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Conn::Conn(std::unique_ptr<ConnAndStmts> connAndStmts, ConnPool* pool)
    : m_connAndStmts(std::move(connAndStmts)),
      m_pool(pool) {
  m_executionStartTime = std::chrono::steady_clock::now();
}

//------------------------------------------------------------------------------
// move constructor
//------------------------------------------------------------------------------
Conn::Conn(Conn&& other) noexcept : m_connAndStmts(std::move(other.m_connAndStmts)), m_pool(other.m_pool) {
  other.m_pool = nullptr;
  m_executionStartTime = std::chrono::steady_clock::now();
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
  } catch (...) {}

  m_pool = nullptr;
  m_connAndStmts.reset();
}

//------------------------------------------------------------------------------
// operator=
//------------------------------------------------------------------------------
Conn& Conn::operator=(Conn&& rhs) noexcept {
  std::swap(m_connAndStmts, rhs.m_connAndStmts);
  std::swap(m_pool, rhs.m_pool);
  return *this;
}

//------------------------------------------------------------------------------
// setAutocommitMode
//------------------------------------------------------------------------------
void Conn::setAutocommitMode(const AutocommitMode autocommitMode) {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    m_connAndStmts->conn->setAutocommitMode(autocommitMode);
  } else {
    throw exception::Exception("Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// getAutocommitMode
//------------------------------------------------------------------------------
AutocommitMode Conn::getAutocommitMode() const {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getAutocommitMode();
  } else {
    throw exception::Exception("Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// createStmt
//------------------------------------------------------------------------------
Stmt Conn::createStmt(const std::string& sql) {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->stmtPool->getStmt(*m_connAndStmts->conn, sql);
  } else {
    throw exception::Exception("Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// executeNonQuery
//------------------------------------------------------------------------------
void Conn::executeNonQuery(const std::string& sql) {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    m_connAndStmts->conn->executeNonQuery(sql);
  } else {
    throw exception::Exception("Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// setDbQuerySummary
//------------------------------------------------------------------------------
void Conn::setDbQuerySummary(const std::string& optQuerySummary) {
  m_executionStartTime = std::chrono::steady_clock::now();
  m_querySummary = optQuerySummary;
};

void Conn::setRowCountForTelemetry(uint64_t rowCount) {
  m_rowCount = rowCount;
}

//------------------------------------------------------------------------------
// commit
//------------------------------------------------------------------------------
void Conn::commit() {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    m_connAndStmts->conn->commit();
    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - m_executionStartTime).count();
    cta::telemetry::metrics::dbClientOperationDuration->Record(
      duration,
      {
        {cta::semconv::attr::kDbSystemName,   m_pool->m_connFactory->getDbSystemName()},
        {cta::semconv::attr::kDbNamespace,    m_pool->m_connFactory->getDbNamespace() },
        {cta::semconv::attr::kDbQuerySummary, m_querySummary                          }
    },
      opentelemetry::context::RuntimeContext::GetCurrent());
    cta::telemetry::metrics::dbClientOperationReturnedRows->Record(
      m_rowCount,
      {
        {cta::semconv::attr::kDbSystemName,   m_pool->m_connFactory->getDbSystemName()},
        {cta::semconv::attr::kDbNamespace,    m_pool->m_connFactory->getDbNamespace() },
        {cta::semconv::attr::kDbQuerySummary, m_querySummary                          }
    },
      opentelemetry::context::RuntimeContext::GetCurrent());

  } else {
    throw exception::Exception("Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// rollback
//------------------------------------------------------------------------------
void Conn::rollback() {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    m_connAndStmts->conn->rollback();
  } else {
    throw exception::Exception("Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// getSchemaColumns
//------------------------------------------------------------------------------
std::map<std::string, std::string, std::less<>> Conn::getColumns(const std::string& tableName) const {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getColumns(tableName);
  } else {
    throw exception::Exception("Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// getTableNames
//------------------------------------------------------------------------------
std::vector<std::string> Conn::getTableNames() const {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getTableNames();
  } else {
    throw exception::Exception("Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// getIndexNames
//------------------------------------------------------------------------------
std::vector<std::string> Conn::getIndexNames() const {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getIndexNames();
  } else {
    throw exception::Exception("Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// closeUnderlyingStmtsAndConn
//------------------------------------------------------------------------------
void Conn::closeUnderlyingStmtsAndConn() {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    m_connAndStmts->stmtPool->clear();
    m_connAndStmts->conn->close();
  } else {
    throw exception::Exception("Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// isOpen
//------------------------------------------------------------------------------
bool Conn::isOpen() const {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->isOpen();
  } else {
    throw exception::Exception("Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// getSequenceNames
//------------------------------------------------------------------------------
std::vector<std::string> Conn::getSequenceNames() {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getSequenceNames();
  } else {
    throw exception::Exception("Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// getTriggerNames
//------------------------------------------------------------------------------
std::vector<std::string> Conn::getTriggerNames() {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getTriggerNames();
  } else {
    throw exception::Exception("Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// getParallelTableNames
//------------------------------------------------------------------------------
std::vector<std::string> Conn::getParallelTableNames() {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getParallelTableNames();
  } else {
    throw exception::Exception("Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// getConstraintNames
//------------------------------------------------------------------------------
std::vector<std::string> Conn::getConstraintNames(const std::string& tableName) {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getConstraintNames(tableName);
  } else {
    throw exception::Exception("Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// getStoredProcedureNames
//------------------------------------------------------------------------------
std::vector<std::string> Conn::getStoredProcedureNames() {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getStoredProcedureNames();
  } else {
    throw exception::Exception("Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// getSynonymNames
//------------------------------------------------------------------------------
std::vector<std::string> Conn::getSynonymNames() {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getSynonymNames();
  } else {
    throw exception::Exception("Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// getTypeNames
//------------------------------------------------------------------------------
std::vector<std::string> Conn::getTypeNames() {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getTypeNames();
  } else {
    throw exception::Exception("Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// getViewNames
//------------------------------------------------------------------------------
std::vector<std::string> Conn::getViewNames() {
  if (nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getViewNames();
  } else {
    throw exception::Exception("Conn does not contain a connection");
  }
}

}  // namespace cta::rdbms
