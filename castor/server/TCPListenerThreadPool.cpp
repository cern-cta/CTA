/******************************************************************************
 *                   TCPListenerThreadPool.cpp
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
 * @(#)$RCSfile: TCPListenerThreadPool.cpp,v $ $Revision: 1.6 $ $Release$ $Date: 2008/08/12 11:46:52 $ $Author: waldron $
 *
 * Listener thread pool based on TCP
 *
 * @author castor dev team
 *****************************************************************************/

// Include Files
#include <signal.h>
#include "castor/server/ListenerThreadPool.hpp"
#include "castor/server/TCPListenerThreadPool.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/io/ServerSocket.hpp"
#include <errno.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::TCPListenerThreadPool::TCPListenerThreadPool
(const std::string poolName,
 castor::server::IThread* thread,
 int listenPort,
 bool listenerOnOwnThread) throw() :
  ListenerThreadPool(poolName, thread, listenPort, listenerOnOwnThread) {}

//------------------------------------------------------------------------------
// bind
//------------------------------------------------------------------------------
void castor::server::TCPListenerThreadPool::bind() throw (castor::exception::Exception) {
  // Create a socket for the server, bind, and listen 
  try {
    m_sock = new castor::io::ServerSocket(m_port, true);
  } catch (castor::exception::Exception e) {
    clog() << ERROR << "Fatal error: cannot bind socket on port " << m_port << ": "
           << sstrerror(e.code()) << std::endl;
    throw e;
  }
}

//------------------------------------------------------------------------------
// listenLoop
//------------------------------------------------------------------------------
void castor::server::TCPListenerThreadPool::listenLoop() {
  for (;;) {
    try {
      // Accept connections
      castor::io::ServerSocket* s = ((castor::io::ServerSocket*)m_sock)->accept();
      // Handle the command
      threadAssign(s);
    }
    catch(castor::exception::Exception any) {
      // Some errors are consider fatal, such as closure of the listening
      // socket resulting in a bad file descriptor during the thread shutdown
      // process. If we encounter this problem we exit the loop. 
      if (any.code() == EBADF) {
        break;
      }
      clog() << ERROR << "Error while accepting connections to port " << m_port << ": "
             << sstrerror(any.code()) << " - " 
	     << any.getMessage().str() << std::endl;
    }
  }
}
