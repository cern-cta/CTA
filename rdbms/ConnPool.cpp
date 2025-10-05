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
#include "rdbms/ConnPool.hpp"
#include "rdbms/wrapper/ConnFactoryFactory.hpp"
#include "common/telemetry/metrics/instruments/RdbmsInstruments.hpp"
#include "common/semconv/Attributes.hpp"

#include <memory>

namespace cta::rdbms {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ConnPool::ConnPool(const Login& login, const uint64_t maxNbConns)
    : m_connFactory(wrapper::ConnFactoryFactory::create(login)),
      m_maxNbConns(maxNbConns),
      m_nbConnsOnLoan(0) {}

ConnPool::~ConnPool() {
  removeNbConnsOnLoan(m_nbConnsOnLoan);
}

//------------------------------------------------------------------------------
// getConn
//------------------------------------------------------------------------------
Conn ConnPool::getConn() {

  if (0 == m_maxNbConns) {
    throw ConnPoolConfiguredWithZeroConns(std::string(__FUNCTION__) +
                                          " failed: ConnPool is configured with zero connections");
  }
  std::unique_ptr<ConnAndStmts> connAndStmts;

  { // artificial scope to limit the lock taken here
    threading::MutexLocker locker(m_connsAndStmtsMutex);

    while (m_connsAndStmts.empty() && m_nbConnsOnLoan == m_maxNbConns) {
      m_connsAndStmtsCv.wait(locker);
    }
    if (m_connsAndStmts.empty()) {
      connAndStmts = std::make_unique<ConnAndStmts>();
      connAndStmts->conn = m_connFactory->create();
      connAndStmts->stmtPool = std::make_unique<StmtPool>();
    } else {
      connAndStmts = std::move(m_connsAndStmts.front());
      m_connsAndStmts.pop_front();
    }
    addNbConnsOnLoan(1);
  }
  // Checks conn->isOpen() after releasing the lock, and if it's closed:
  // just replaces the conn and stmtPool inside the existing ConnAndStmts
  if (!connAndStmts->conn->isOpen()) {
    connAndStmts->conn = m_connFactory->create();
    connAndStmts->stmtPool = std::make_unique<StmtPool>();
  }
  return Conn(std::move(connAndStmts), this);
}

//------------------------------------------------------------------------------
// returnConn
//------------------------------------------------------------------------------
void ConnPool::returnConn(std::unique_ptr<ConnAndStmts> connAndStmts) {
  try {
    // If the connection is open
    if (connAndStmts->conn->isOpen()) {
      // Try to commit the connection and put it back in the pool
      try {
        connAndStmts->conn->commit();
      } catch (...) {
        // If the commit failed then destroy any prepare statements and then
        // close the connection
        try {
          connAndStmts->stmtPool->clear();
          connAndStmts->conn->close();
        } catch (...) {
          // Ignore any exceptions
        }

        // A closed connection is rare and usually means the underlying TCP/IP
        // connection, if there is one, has been lost.  Delete all the connections
        // currently in the pool because their underlying TCP/IP connections may
        // also have been lost.
        threading::MutexLocker locker(m_connsAndStmtsMutex);
        while (!m_connsAndStmts.empty()) {
          m_connsAndStmts.pop_front();
        }
        removeNbConnsOnLoan(1);
        m_connsAndStmtsCv.signal();
        return;
      }

      // Sets the autocommit mode of the connection to AUTOCOMMIT_ON because
      // this is the default value of a newly created connection
      connAndStmts->conn->setAutocommitMode(AutocommitMode::AUTOCOMMIT_ON);

      threading::MutexLocker locker(m_connsAndStmtsMutex);
      removeNbConnsOnLoan(1);
      m_connsAndStmts.push_back(std::move(connAndStmts));
      m_connsAndStmtsCv.signal();

      // Else the connection is closed
    } else {
      // A closed connection is rare and usually means the underlying TCP/IP
      // connection, if there is one, has been lost.  Delete all the connections
      // currently in the pool because their underlying TCP/IP connections may
      // also have been lost.
      threading::MutexLocker locker(m_connsAndStmtsMutex);
      while (!m_connsAndStmts.empty()) {
        m_connsAndStmts.pop_front();
      }
      removeNbConnsOnLoan(1);
      m_connsAndStmtsCv.signal();
    }
  } catch (exception::Exception& ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// addNbConnsOnLoan
//------------------------------------------------------------------------------
void ConnPool::addNbConnsOnLoan(uint64_t nbConns) {
  m_nbConnsOnLoan += nbConns;
  cta::telemetry::metrics::dbClientConnectionCount->Add(nbConns, {{cta::semconv::attr::kDbSystemName, m_connFactory->getDbSystemName()}, {cta::semconv::attr::kDbNamespace, m_connFactory->getDbNamespace()}});
}

//------------------------------------------------------------------------------
// removeNbConnsOnLoan
//------------------------------------------------------------------------------
void ConnPool::removeNbConnsOnLoan(uint64_t nbConns) {
  // While hypothetically we could use addNbConnsOnLoan with a negative integer instead of using this separate method,
  // doing is error prone due to m_nbConnsOnLoan being unsigned while nbConnes would not be.
  if (nbConns > m_nbConnsOnLoan) {
    throw exception::Exception("Would have reached a negative number connections on loan");
  }
  m_nbConnsOnLoan -= nbConns;
  cta::telemetry::metrics::dbClientConnectionCount->Add(-nbConns, {{cta::semconv::attr::kDbSystemName, m_connFactory->getDbSystemName()}, {cta::semconv::attr::kDbNamespace, m_connFactory->getDbNamespace()}});
}


}  // namespace cta::rdbms
