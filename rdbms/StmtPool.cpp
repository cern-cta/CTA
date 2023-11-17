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
