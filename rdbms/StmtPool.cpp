/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/Exception.hpp"
#include "common/threading/MutexLocker.hpp"
#include "rdbms/wrapper/ConnWrapper.hpp"
#include "rdbms/StmtPool.hpp"

namespace cta::rdbms {

//------------------------------------------------------------------------------
// getStmt
//------------------------------------------------------------------------------
Stmt StmtPool::getStmt(wrapper::ConnWrapper &conn, const std::string &sql) {
  threading::MutexLocker locker(m_stmtsMutex);

  auto itor = m_stmts.find(sql);

  // If there is no prepared statement in the cache
  if(itor == m_stmts.end()) {
    auto stmt = conn.createStmt(sql);
    return Stmt(std::move(stmt), *this);
  } else {
    auto &stmtList = itor->second;
    if(stmtList.empty()) {
      throw exception::Exception(std::string(__FUNCTION__) + " failed: Unexpected empty list of cached statements");
    }
    auto stmt = std::move(stmtList.front());
    stmtList.pop_front();

    // If there are no more cached prepared statements then remove the empty list from the cache
    if(stmtList.empty()) {
      m_stmts.erase(itor);
    }

    return Stmt(std::move(stmt), *this);
  }
}

//------------------------------------------------------------------------------
// getNbStmts
//------------------------------------------------------------------------------
uint64_t StmtPool::getNbStmts() const {
  threading::MutexLocker locker(m_stmtsMutex);

  uint64_t nbStmts = 0;
  for(const auto &maplet: m_stmts) {
    auto &stmtList = maplet.second;
    nbStmts += stmtList.size();
  }
  return nbStmts;
}

//------------------------------------------------------------------------------
// returnStmt
//------------------------------------------------------------------------------
void StmtPool::returnStmt(std::unique_ptr<wrapper::StmtWrapper> stmt) {
  threading::MutexLocker locker(m_stmtsMutex);

  stmt->clear();

  m_stmts[stmt->getSql()].push_back(std::move(stmt));
}

//------------------------------------------------------------------------------
// clear
//------------------------------------------------------------------------------
void StmtPool::clear() {
  m_stmts.clear();
}

} // namespace cta::rdbms
