/******************************************************************************
 *                  UDPListenerThreadPool.cpp
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
 * @(#)$RCSfile: UDPListenerThreadPool.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2007/04/12 16:54:03 $ $Author: itglp $
 *
 * A listener thread pool listening on an UDP port
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Include Files
#include <signal.h>
#include "castor/server/ListenerThreadPool.hpp"
#include "castor/server/UDPListenerThreadPool.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/io/UDPSocket.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::UDPListenerThreadPool::UDPListenerThreadPool
(const std::string poolName,
 castor::server::IThread* thread,
 int listenPort,
 bool listenerOnOwnThread) throw() :
  ListenerThreadPool(poolName, thread, listenPort, listenerOnOwnThread) {}

//------------------------------------------------------------------------------
// init
//------------------------------------------------------------------------------
void castor::server::UDPListenerThreadPool::init() throw (castor::exception::Exception) {
  castor::server::ListenerThreadPool::init();  
  /* Create a socket for the server, bind, and listen */
  try {
    m_sock = new castor::io::UDPSocket(m_port, true);
  } catch (castor::exception::Exception e) {
    clog() << ERROR << "Fatal error: cannot bind UDP socket on port " << m_port << ": "
           << e.getMessage().str() << std::endl;
    throw e;         // calling server should exit() here
  }
}

//------------------------------------------------------------------------------
// listenLoop
//------------------------------------------------------------------------------
void castor::server::UDPListenerThreadPool::listenLoop() {
  for (;;) {
    try {
      // Read next datagram
      castor::IObject* obj = m_sock->readObject();
      // handle the command
      threadAssign(obj);
    } catch (castor::exception::Exception e) {
      clog() << ERROR << "Error while reading datagrams from port " << m_port << ": "
             << e.getMessage().str() << std::endl;
    }
  }
}
