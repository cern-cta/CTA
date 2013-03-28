/******************************************************************************
 *                      AuthListenerThreadPool.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2004  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 * A ListenerThreadPool which uses AuthSockets to handle the connections
 *
 * @author Giuseppe Lo Presti
 ******************************************************** *********************/

// Include Files
#include <signal.h>
#include "castor/io/AuthServerSocket.hpp"
#include "castor/server/AuthListenerThreadPool.hpp"
#include "castor/exception/Exception.hpp"

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::server::AuthListenerThreadPool::AuthListenerThreadPool
(const std::string poolName,
 castor::server::IThread* thread,
 unsigned int listenPort,
 bool waitIfBusy,
 unsigned int nbThreads) throw(castor::exception::Exception) :
  TCPListenerThreadPool(poolName, thread, listenPort, waitIfBusy, nbThreads) {}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::server::AuthListenerThreadPool::AuthListenerThreadPool
(const std::string poolName,
 castor::server::IThread* thread,
 unsigned int listenPort,
 bool waitIfBusy,
 unsigned int initThreads,
 unsigned int maxThreads,
 unsigned int threshold,
 unsigned int maxTasks) throw(castor::exception::Exception) :
  TCPListenerThreadPool(poolName, thread, listenPort, waitIfBusy,
                        initThreads, maxThreads, threshold, maxTasks) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::server::AuthListenerThreadPool::~AuthListenerThreadPool() throw() {}

//------------------------------------------------------------------------------
// bind
//------------------------------------------------------------------------------
void castor::server::AuthListenerThreadPool::bind()
  throw (castor::exception::Exception) {
  // Create a secure socket for the server, bind, and listen
  m_sock = new castor::io::AuthServerSocket(m_port, true);
}
