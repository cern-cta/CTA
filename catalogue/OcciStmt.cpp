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

// Version 12.1 of oracle instant client uses the pre _GLIBCXX_USE_CXX11_ABI
#define _GLIBCXX_USE_CXX11_ABI 0

#include "catalogue/OcciConn.hpp"
#include "catalogue/OcciRset.hpp"
#include "catalogue/OcciStmt.hpp"

#include <cstring>
#include <iostream>
#include <map>
#include <sstream>
#include <stdexcept>

namespace cta {
namespace catalogue {

class OcciStmt::ParamNameToIdx {
public:
  /**
   * Constructor.
   *
   * Parses the specified SQL statement to populate an internal map from SQL
   * parameter name to parameter index.
   *
   * @param sql The SQL statement to be parsed for SQL parameter names.
   */
  ParamNameToIdx(const char *const sql) {
    bool waitingForAParam = true;
    std::ostringstream paramName;
    unsigned int paramIdx = 1;

    for(const char *ptr = sql; ; ptr++) {
      if(waitingForAParam) {
        if('\0' == *ptr) {
          break;
        } else if(':' == *ptr) {
          waitingForAParam = false;
          paramName << ":";
        }
      } else {
        if(!isValidParamNameChar(*ptr)) {
          if(paramName.str().empty()) {
            throw std::runtime_error("Parse error: Empty SQL parameter name");
          }
          if(m_nameToIdx.find(paramName.str()) != m_nameToIdx.end()) {
            throw std::runtime_error("Parse error: SQL parameter " + paramName.str() + " is a duplicate");
          }
          m_nameToIdx[paramName.str()] = paramIdx;
          paramName.clear();
          paramIdx++;
          waitingForAParam = true;
        }

        if('\0' == *ptr) {
          break;
        }

        if(':' == *ptr) {
          throw std::runtime_error("Parse error: Consecutive SQL parameter names are not permitted");
        } else {
          paramName << *ptr;
        }
      }
    }
  }

  /**
   * Returns the index of teh specified SQL parameter.
   *
   * @param paramNAme The name of the SQL parameter.
   * @return The index of the SQL parameter.
   */
  unsigned int getIdx(const char *const paramName) const {
    auto itor = m_nameToIdx.find(paramName);
    if(itor == m_nameToIdx.end()) {
      throw std::runtime_error(std::string(__FUNCTION__) + " failed: The SQL parameter " + paramName +
        " does not exist");
    }
    return itor->second;
  }

private:

  /**
   * Map from SQL parameter name to parameter index.
   */
  std::map<std::string, unsigned int> m_nameToIdx;

  bool isValidParamNameChar(const char c) {
    return ('0' <= c && c <= '9') ||
           ('A' <= c && c <= 'Z') ||
           ('a' <= c && c <= 'z') ||
           c == '_';
  }
};

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OcciStmt::OcciStmt(const char *const sql, OcciConn &conn, oracle::occi::Statement *const stmt) :
  m_conn(conn),
  m_stmt(stmt) {
  if(NULL == sql) {
    throw std::runtime_error(std::string(__FUNCTION__) + " failed: sql is NULL");
  }
  if(NULL == stmt) {
    throw std::runtime_error(std::string(__FUNCTION__) + " failed for SQL statement " + sql +
      ": stmt is NULL");
  }

  // Work with C strings because they haven't changed with respect to _GLIBCXX_USE_CXX11_ABI
  const std::size_t sqlLen = std::strlen(sql);
  m_sql.reset(new char[sqlLen + 1]);
  std::memcpy(m_sql.get(), sql, sqlLen);
  m_sql[sqlLen] = '\0';

  m_paramNameToIdx.reset(new ParamNameToIdx(sql));
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
OcciStmt::~OcciStmt() throw() {
  try {
    close(); // Idempotent close() method
  } catch (...) {
    // Destructor does not throw
  }
}

//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void OcciStmt::close() {
  std::lock_guard<std::mutex> lock(m_mutex);

  if (NULL != m_stmt) {
    m_conn->terminateStatement(m_stmt);
    m_stmt = NULL;
  }
}

//------------------------------------------------------------------------------
// getSql
//------------------------------------------------------------------------------
const char *OcciStmt::getSql() const {
  return m_sql.get();
}

//------------------------------------------------------------------------------
// bind
//------------------------------------------------------------------------------
void OcciStmt::bind(const char *paramName, const uint64_t paramValue) {
  try {
    const unsigned paramIdx = m_paramNameToIdx->getIdx(paramName);
    m_stmt->setUInt(paramIdx, paramValue);
  } catch(std::exception &ne) {
    throw std::runtime_error(std::string(__FUNCTION__) + " failed: " + ne.what());
  }
}

//------------------------------------------------------------------------------
// bind
//------------------------------------------------------------------------------
void OcciStmt::bind(const char *paramName, const char *paramValue) {
  try {
    const unsigned paramIdx = m_paramNameToIdx->getIdx(paramName);
    m_stmt->setString(paramIdx, paramValue);
  } catch(std::exception &ne) {
    throw std::runtime_error(std::string(__FUNCTION__) + " failed: " + ne.what());
  }
}

//------------------------------------------------------------------------------
// executeQuery
//------------------------------------------------------------------------------
DbRset *OcciStmt::executeQuery() {
  using namespace oracle;

  try {
    return new OcciRset(*this, m_stmt->executeQuery());
  } catch(std::exception &ne) {
    throw std::runtime_error(std::string(__FUNCTION__) + " failed for SQL statement " + getSql() + ": " + ne.what());
  }
}

//------------------------------------------------------------------------------
// get
//------------------------------------------------------------------------------
oracle::occi::Statement *OcciStmt::get() const {
  return m_stmt;
}

//------------------------------------------------------------------------------
// operator->
//------------------------------------------------------------------------------
oracle::occi::Statement *OcciStmt::operator->() const {
  return get();
}

} // namespace catalogue
} // namespace cta
