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
#include "common/threading/MutexLocker.hpp"
#include "rdbms/ConnPool.hpp"

#include <memory>

namespace cta {
namespace rdbms {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ConnPool::ConnPool(ConnFactory &connFactory, const uint64_t maxNbConns):
  m_connFactory(connFactory),
  m_maxNbConns(maxNbConns),
  m_nbConnsOnLoan(0) {
  try {
    createConns(m_maxNbConns);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// createConns
//------------------------------------------------------------------------------
void ConnPool::createConns(const uint64_t nbConns) {
  try {
    for(uint64_t i = 0; i < nbConns; i++) {
      m_conns.push_back(m_connFactory.create());
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getConn
//------------------------------------------------------------------------------
PooledConn ConnPool::getConn() {
  threading::MutexLocker locker(m_connsMutex);

  while(m_conns.size() == 0 && m_nbConnsOnLoan == m_maxNbConns) {
    m_connsCv.wait(locker);
  }

  if(m_conns.size() == 0) {
    m_conns.push_back(m_connFactory.create());
  }

  std::unique_ptr<Conn> conn = std::move(m_conns.front());
  m_conns.pop_front();
  m_nbConnsOnLoan++;
  if(conn->isOpen()) {
    return PooledConn(std::move(conn), this);
  } else {
    return PooledConn(m_connFactory.create(), this);
  }
}

//------------------------------------------------------------------------------
// returnConn
//------------------------------------------------------------------------------
void ConnPool::returnConn(std::unique_ptr<Conn> conn) {
  try {
    // If the connection is open
    if(conn->isOpen()) {

      // Try to commit the connection and put it back in the pool
      try {
        conn->commit();
      } catch(...) {
        // If the commit failed then close the connection
        try {
          conn->close();
        } catch(...) {
          // Ignore any exceptions
        }

        // A closed connection is rare and usually means the underlying TCP/IP
        // connection, if there is one, has been lost.  Delete all the connections
        // currently in the pool because their underlying TCP/IP connections may
        // also have been lost.
        threading::MutexLocker locker(m_connsMutex);
        while(!m_conns.empty()) {
          m_conns.pop_front();
        }
        if(0 == m_nbConnsOnLoan) {
          throw exception::Exception("Would have reached -1 connections on loan");
        }
        m_nbConnsOnLoan--;
        m_connsCv.signal();
        return;
      }

      threading::MutexLocker locker(m_connsMutex);
      if(0 == m_nbConnsOnLoan) {
        throw exception::Exception("Would have reached -1 connections on loan");
      }
      m_nbConnsOnLoan--;
      m_conns.push_back(std::move(conn));
      m_connsCv.signal();

    // Else the connection is closed
    } else {

      // A closed connection is rare and usually means the underlying TCP/IP
      // connection, if there is one, has been lost.  Delete all the connections
      // currently in the pool because their underlying TCP/IP connections may
      // also have been lost.
      threading::MutexLocker locker(m_connsMutex);
      while(!m_conns.empty()) {
        m_conns.pop_front();
      }
      if(0 == m_nbConnsOnLoan) {
        throw exception::Exception("Would have reached -1 connections on loan");
      }
      m_nbConnsOnLoan--;
      m_connsCv.signal();
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

} // namespace rdbms
} // namespace cta
