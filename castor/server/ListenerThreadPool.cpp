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
 * @(#)$RCSfile: ListenerThreadPool.cpp,v $ $Revision: 1.14 $ $Release$ $Date: 2008/02/01 11:20:27 $ $Author: itglp $
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
#include "Cinit.h"
#include "Cpool_api.h"
#include "castor/logstream.h"
#include <iomanip>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::ListenerThreadPool::ListenerThreadPool(const std::string poolName,
                                               castor::server::IThread* thread,
                                               int listenPort,
                                               bool listenerOnOwnThread) throw() :
  BaseThreadPool(poolName, thread), m_sock(0), m_port(listenPort),
  m_spawnListener(listenerOnOwnThread) {}

//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::server::ListenerThreadPool::run() throw (castor::exception::Exception) {  
  // bind the socket (this can fail, we just propagate the exception)
  bind();

  // create the thread pool
  if(m_nbThreads > 0) {
    int actualNbThreads;
    m_threadPoolId = Cpool_create(m_nbThreads, &actualNbThreads);
    if (m_threadPoolId < 0) {
      castor::exception::Internal ex;
      ex.getMessage() << "Listener thread pool '" << m_poolName << "' creation error: "
             << m_threadPoolId << std::endl;
      clog() << ALERT << ex.getMessage().str();
      throw ex;
    }
    else {
      m_nbThreads = actualNbThreads;
      clog() << DEBUG << "Listener thread pool '" << m_poolName << "' created with "
             << m_nbThreads << " threads" << std::endl;
    }
  }
  else {
    // this is allowed, cf. threadAssign()
    clog() << DEBUG << "Listener thread '" << m_poolName 
           << "' created with no worker threads" << std::endl;
  }
  
  // Initialize the underlying thread. We do it here once for the whole thread
  // pool instead of once per thread as the threads are created in a suspended
  // state and will get some work only when the listener socket will have
  // something to dispatch
  m_thread->init();
  
  // start the listening loop
  if(m_spawnListener || m_nbThreads == 0) {
    Cthread_create_detached(castor::server::_listener_run, this);
  } else {
    listenLoop();
  }
}

//------------------------------------------------------------------------------
// shutdown
//------------------------------------------------------------------------------
bool castor::server::ListenerThreadPool::shutdown() throw() {
  // stop accepting connections
  if(m_sock != 0) {
    m_sock->close();
  }
  
  // For the time being we assume threads will end very soon, so we just inherit
  // the default behavior.
  // A better implementation is to call Cpool_next_index_timeout(m_threadPoolId, 0)
  // until it says that all threads are idle.
  return castor::server::BaseThreadPool::shutdown();
}

//------------------------------------------------------------------------------
// _listener_run
//------------------------------------------------------------------------------
void* castor::server::_listener_run(void* param) {
  castor::server::ListenerThreadPool* tp = (castor::server::ListenerThreadPool*)param;
  tp->listenLoop();
  return 0;
}

//------------------------------------------------------------------------------
// threadAssign
//------------------------------------------------------------------------------
void castor::server::ListenerThreadPool::threadAssign(void *param) {
  // Initializing the arguments to pass to the static request processor
  struct threadArgs *args = new threadArgs();
  args->handler = this;
  args->param = param;
  if (m_nbThreads > 0) {
    int assign_rc = Cpool_assign(m_threadPoolId,
                                 &castor::server::_thread_run,
                                 args,
                                 -1);
    if (assign_rc < 0) {
      clog() << "Error while spawning thread in pool " << m_poolName << std::endl;
    }
  } else {
    // In this case we run the user thread code in the same thread.
    // Note that during the user thread execution we cannot listen.
    castor::server::_thread_run(args);
  }
}

