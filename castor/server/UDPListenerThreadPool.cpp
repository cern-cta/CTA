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
 * @(#)$RCSfile: UDPListenerThreadPool.cpp,v $ $Revision: 1.9 $ $Release$ $Date: 2009/08/10 15:27:31 $ $Author: itglp $
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
#include <errno.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::UDPListenerThreadPool::UDPListenerThreadPool
(const std::string poolName,
 castor::server::IThread* thread,
 int listenPort,
 bool waitIfBusy,
 unsigned nbThreads) throw() :
  ListenerThreadPool(poolName, thread, listenPort, waitIfBusy, nbThreads) {}

//------------------------------------------------------------------------------
// bind
//------------------------------------------------------------------------------
void castor::server::UDPListenerThreadPool::bind()
  throw (castor::exception::Exception) {
  m_sock = new castor::io::UDPSocket(m_port, true, true);
}

//------------------------------------------------------------------------------
// listenLoop
//------------------------------------------------------------------------------
void castor::server::UDPListenerThreadPool::listenLoop() {
  for (;;) {
    try {
      // Read next datagram
      castor::IObject* obj = ((castor::io::UDPSocket*)m_sock)->readObject();
      if(m_stopped) {
        // we just got stopped, don't bother processing as we're terminating
        break;
      }
      // Handle the command
      threadAssign(obj);
    } catch (castor::exception::Exception any) {
      // Some errors are considered fatal, such as closure of the listening
      // socket resulting in a bad file descriptor during the thread shutdown
      // process. If we encounter this problem we exit the loop.
      if (any.code() == EBADF) {
        break;
      }
      // "Error while reading datagrams"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Port", m_port),
         castor::dlf::Param("Error", sstrerror(any.code())),
         castor::dlf::Param("Message", any.getMessage().str())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
                              DLF_BASE_FRAMEWORK + 1, 3, params);
    }
  }
}

//------------------------------------------------------------------------------
// terminate
//------------------------------------------------------------------------------
void castor::server::UDPListenerThreadPool::terminate(void* param) throw() {
  castor::IObject* obj = (castor::IObject*)param;
  // UDP is best-effort, don't bother contacting the client but free allocated memory
  delete obj;
}

