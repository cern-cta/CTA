/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rdbms/Stmt.hpp"

#include "common/Timer.hpp"
#include "common/exception/Exception.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/telemetry/metrics/instruments/RdbmsInstruments.hpp"
#include "rdbms/StmtPool.hpp"
#include "rdbms/wrapper/StmtWrapper.hpp"

#include <opentelemetry/context/runtime_context.h>

namespace cta::rdbms {

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
Stmt::Stmt() : m_stmtPool(nullptr) {}

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
Stmt::Stmt(std::unique_ptr<wrapper::StmtWrapper> stmt, StmtPool& stmtPool)
    : m_stmt(std::move(stmt)),
      m_stmtPool(&stmtPool) {}

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
Stmt::Stmt(Stmt&& other) noexcept : m_stmt(std::move(other.m_stmt)), m_stmtPool(other.m_stmtPool) {}

//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
Stmt::~Stmt() noexcept {
  reset();
}

//-----------------------------------------------------------------------------
// reset
//-----------------------------------------------------------------------------
void Stmt::reset() noexcept {
  try {
    // If this smart prepared statement currently points to a prepared
    // statement then return it back to its pool
    if (nullptr != m_stmtPool && nullptr != m_stmt) {
      m_stmtPool->returnStmt(std::move(m_stmt));
    }
  } catch (...) {
    // Ignore any exceptions
  }

  m_stmtPool = nullptr;
  m_stmt.reset();
}

//-----------------------------------------------------------------------------
// operator=
//-----------------------------------------------------------------------------
Stmt& Stmt::operator=(Stmt&& rhs) noexcept {
  std::swap(m_stmt, rhs.m_stmt);
  std::swap(m_stmtPool, rhs.m_stmtPool);
  return *this;
}

//-----------------------------------------------------------------------------
// getSql
//-----------------------------------------------------------------------------
const std::string& Stmt::getSql() const {
  if (nullptr != m_stmt) {
    return m_stmt->getSql();
  } else {
    throw exception::Exception("Stmt does not contain a cached statement");
  }
}

//-----------------------------------------------------------------------------
// getParamIdx
//-----------------------------------------------------------------------------
uint32_t Stmt::getParamIdx(const std::string& paramName) const {
  if (nullptr != m_stmt) {
    return m_stmt->getParamIdx(paramName);
  } else {
    throw exception::Exception("Stmt does not contain a cached statement");
  }
}

//-----------------------------------------------------------------------------
// bindUint8
//-----------------------------------------------------------------------------
void Stmt::bindUint8(const std::string& paramName, const std::optional<uint8_t>& paramValue) {
  if (nullptr != m_stmt) {
    return m_stmt->bindUint8(paramName, paramValue);
  } else {
    throw exception::Exception("Stmt does not contain a cached statement");
  }
}

//-----------------------------------------------------------------------------
// bindUint16
//-----------------------------------------------------------------------------
void Stmt::bindUint16(const std::string& paramName, const std::optional<uint16_t>& paramValue) {
  if (nullptr != m_stmt) {
    return m_stmt->bindUint16(paramName, paramValue);
  } else {
    throw exception::Exception("Stmt does not contain a cached statement");
  }
}

//-----------------------------------------------------------------------------
// bindUint32
//-----------------------------------------------------------------------------
void Stmt::bindUint32(const std::string& paramName, const std::optional<uint32_t>& paramValue) {
  if (nullptr != m_stmt) {
    return m_stmt->bindUint32(paramName, paramValue);
  } else {
    throw exception::Exception("Stmt does not contain a cached statement");
  }
}

//-----------------------------------------------------------------------------
// bindUint64
//-----------------------------------------------------------------------------
void Stmt::bindUint64(const std::string& paramName, const std::optional<uint64_t>& paramValue) {
  if (nullptr != m_stmt) {
    return m_stmt->bindUint64(paramName, paramValue);
  } else {
    throw exception::Exception("Stmt does not contain a cached statement");
  }
}

//-----------------------------------------------------------------------------
// bindDouble
//-----------------------------------------------------------------------------
void Stmt::bindDouble(const std::string& paramName, const std::optional<double>& paramValue) {
  if (nullptr != m_stmt) {
    return m_stmt->bindDouble(paramName, paramValue);
  } else {
    throw exception::Exception("Stmt does not contain a cached statement");
  }
}

//-----------------------------------------------------------------------------
// bindBool
//-----------------------------------------------------------------------------
void Stmt::bindBool(const std::string& paramName, const std::optional<bool>& paramValue) {
  if (nullptr != m_stmt) {
    return m_stmt->bindBool(paramName, paramValue);
  } else {
    throw exception::Exception("Stmt does not contain a cached statement");
  }
}

//-----------------------------------------------------------------------------
// bindBlob
//-----------------------------------------------------------------------------
void Stmt::bindBlob(const std::string& paramName, const std::string& paramValue) {
  if (nullptr != m_stmt) {
    return m_stmt->bindBlob(paramName, paramValue);
  } else {
    throw exception::Exception("Stmt does not contain a cached statement");
  }
}

//-----------------------------------------------------------------------------
// bindString
//-----------------------------------------------------------------------------
void Stmt::bindString(const std::string& paramName, const std::optional<std::string>& paramValue) {
  if (nullptr != m_stmt) {
    return m_stmt->bindString(paramName, paramValue);
  } else {
    throw exception::Exception("Stmt does not contain a cached statement");
  }
}

//-----------------------------------------------------------------------------
// resetQuery
//-----------------------------------------------------------------------------
void Stmt::resetQuery() {
  if (nullptr != m_stmt) {
    return m_stmt->resetQuery();
  } else {
    throw exception::Exception("Stmt does not contain a cached statement");
  }
}

//-----------------------------------------------------------------------------
// executeQuery
//-----------------------------------------------------------------------------
Rset Stmt::executeQuery() {
  utils::Timer timer;
  try {
    if (nullptr != m_stmt) {
      auto result = Rset(m_stmt->executeQuery());

      cta::telemetry::metrics::dbClientOperationDuration->Record(
        timer.msecs(),
        {
          {cta::semconv::attr::kDbSystemName,   m_stmt->getDbSystemName()  },
          {cta::semconv::attr::kDbNamespace,    m_stmt->getDbNamespace()   },
          {cta::semconv::attr::kDbQuerySummary, m_stmt->getDbQuerySummary()}
      },
        opentelemetry::context::RuntimeContext::GetCurrent());
      return result;
    } else {
      throw exception::Exception("Stmt does not contain a cached statement");
    }
  } catch (std::exception&) {
    cta::telemetry::metrics::dbClientOperationDuration->Record(
      timer.msecs(),
      {
        {cta::semconv::attr::kDbSystemName,   m_stmt->getDbSystemName()                      },
        {cta::semconv::attr::kDbNamespace,    m_stmt->getDbNamespace()                       },
        {cta::semconv::attr::kErrorType,      cta::semconv::attr::ErrorTypeValues::kException},
        {cta::semconv::attr::kDbQuerySummary, m_stmt->getDbQuerySummary()                    }
    },
      opentelemetry::context::RuntimeContext::GetCurrent());
    throw;
  }
}

//-----------------------------------------------------------------------------
// executeNonQuery
//-----------------------------------------------------------------------------
void Stmt::executeNonQuery() {
  utils::Timer timer;
  try {
    if (nullptr != m_stmt) {
      m_stmt->executeNonQuery();
      cta::telemetry::metrics::dbClientOperationDuration->Record(
        timer.msecs(),
        {
          {cta::semconv::attr::kDbSystemName,   m_stmt->getDbSystemName()  },
          {cta::semconv::attr::kDbNamespace,    m_stmt->getDbNamespace()   },
          {cta::semconv::attr::kDbQuerySummary, m_stmt->getDbQuerySummary()}
      },
        opentelemetry::context::RuntimeContext::GetCurrent());
      uint64_t nrows = m_stmt->getNbAffectedRows();
      cta::telemetry::metrics::dbClientOperationReturnedRows->Record(
        nrows,
        {
          {cta::semconv::attr::kDbSystemName,   m_stmt->getDbSystemName()  },
          {cta::semconv::attr::kDbNamespace,    m_stmt->getDbNamespace()   },
          {cta::semconv::attr::kDbQuerySummary, m_stmt->getDbQuerySummary()}
      },
        opentelemetry::context::RuntimeContext::GetCurrent());

    } else {
      throw exception::Exception("Stmt does not contain a cached statement");
    }
  } catch (std::exception&) {
    cta::telemetry::metrics::dbClientOperationDuration->Record(
      timer.msecs(),
      {
        {cta::semconv::attr::kDbSystemName,   m_stmt->getDbSystemName()                      },
        {cta::semconv::attr::kDbNamespace,    m_stmt->getDbNamespace()                       },
        {cta::semconv::attr::kErrorType,      cta::semconv::attr::ErrorTypeValues::kException},
        {cta::semconv::attr::kDbQuerySummary, m_stmt->getDbQuerySummary()                    }
    },
      opentelemetry::context::RuntimeContext::GetCurrent());
    throw;
  }
}

//-----------------------------------------------------------------------------
// getNbAffectedRows
//-----------------------------------------------------------------------------
uint64_t Stmt::getNbAffectedRows() const {
  if (nullptr != m_stmt) {
    return m_stmt->getNbAffectedRows();
  } else {
    throw exception::Exception("Stmt does not contain a cached statement");
  }
}

//-----------------------------------------------------------------------------
// getStmt
//-----------------------------------------------------------------------------
wrapper::StmtWrapper& Stmt::getStmt() {
  if (nullptr != m_stmt) {
    return *m_stmt;
  } else {
    throw exception::Exception("Stmt does not contain a cached statement");
  }
}

//------------------------------------------------------------------------------
// setDbQuerySummary
//------------------------------------------------------------------------------
void Stmt::setDbQuerySummary(const std::string& optQuerySummary) {
  m_stmt->setDbQuerySummary(optQuerySummary);
}

}  // namespace cta::rdbms
