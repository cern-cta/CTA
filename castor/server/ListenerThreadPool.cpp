/******************************************************************************
 *                    ListenerThreadPool.cpp
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
 * @(#)$RCSfile: ListenerThreadPool.cpp,v $ $Revision: 1.17 $ $Release$ $Date: 2009/03/26 14:26:20 $ $Author: itglp $
 *
 * Abstract class defining a listener thread pool
 *
 * @author castor dev team
 *****************************************************************************/

// Include Files
#include <signal.h>
#include "castor/server/BaseThreadPool.hpp"
#include "castor/server/ListenerThreadPool.hpp"
#include "castor/exception/Internal.hpp"
#include "Cthread_api.h"
#include "castor/logstream.h"
#include <iomanip>

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::server::ListenerThreadPool::ListenerThreadPool(const std::string poolName,
                                               castor::server::IThread* thread,
                                               unsigned int listenPort,
                                               bool listenerOnOwnThread,
                                               unsigned int nbThreads)
  throw(castor::exception::Exception) :
  DynamicThreadPool(poolName, thread, nbThreads, nbThreads),
  m_sock(0), m_port(listenPort), m_spawnListener(listenerOnOwnThread) {}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::server::ListenerThreadPool::ListenerThreadPool(const std::string poolName,
                                               castor::server::IThread* thread,
                                               unsigned int listenPort,
                                               bool listenerOnOwnThread,
                                               unsigned int initThreads,
                                               unsigned int maxThreads,
                                               unsigned int threshold,
                                               unsigned int maxTasks)
  throw(castor::exception::Exception) :
  DynamicThreadPool(poolName, thread, initThreads, maxThreads, threshold, maxTasks),
  m_sock(0), m_port(listenPort), m_spawnListener(listenerOnOwnThread) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::server::ListenerThreadPool::~ListenerThreadPool() throw() {}
  
//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::server::ListenerThreadPool::run() throw (castor::exception::Exception) {  
  // bind the socket (this can fail, we just propagate the exception)
  bind();

  // create the thread pool
  DynamicThreadPool::run();
  
  // start the listening loop
  if(m_spawnListener || m_nbThreads == 0) {
    Cthread_create_detached((void *(*)(void *))&_listener, this);
  } else {
    listenLoop();
  }
}

//------------------------------------------------------------------------------
// shutdown
//------------------------------------------------------------------------------
bool castor::server::ListenerThreadPool::shutdown(bool wait) throw() {
  // stop accepting connections
  if(m_sock != 0) {
    try {
      m_sock->close();
    } 
    catch(castor::exception::Exception ignored) {}
  }
  
  return castor::server::DynamicThreadPool::shutdown(wait);
}

//------------------------------------------------------------------------------
// _listener
//------------------------------------------------------------------------------
void* castor::server::ListenerThreadPool::_listener(void* param) {
  castor::server::ListenerThreadPool* tp = (castor::server::ListenerThreadPool*)param;
  tp->listenLoop();
  return 0;
}

//------------------------------------------------------------------------------
// threadAssign
//------------------------------------------------------------------------------
void castor::server::ListenerThreadPool::threadAssign(void *param) {
  if (m_nbThreads == 0) {
    // In this case we run the user thread code in the same thread.
    // Note that during the user thread execution we cannot accept connections.
    try {
      m_thread->run(param);
    } catch(...) {
      clog() << ERROR << "Uncaught GENERAL exception in a thread from pool "
             << m_poolName << std::endl;
    }
    return;
  }
  
  // Otherwise, dispatch the task
  try {
    addTask(param, false);
  } catch(castor::exception::Exception &e) {
    if(e.code() == EAGAIN) {
      // the thread pool is exhausted
      clog() << ERROR << "No idle thread in pool " << m_poolName 
             << " to dispatch this request to at the moment" << std::endl;
      terminate(param);
    }
    else {
      // just log any other exception
      clog() << ERROR << "Error while dispatching to a thread in pool " 
      << m_poolName << " : " << e.getMessage().str(); 
    }
  }
}
