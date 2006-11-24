/******************************************************************************
 *                      ListenerThreadPool.cpp
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
 * @(#)$RCSfile: ListenerThreadPool.cpp,v $ $Revision: 1.7 $ $Release$ $Date: 2006/11/24 11:28:45 $ $Author: itglp $
 *
 *
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Include Files
#include <signal.h>
#include "castor/server/ListenerThreadPool.hpp"
#include "castor/MsgSvc.hpp"
#include "castor/Services.hpp"
#include "castor/exception/Internal.hpp"
#include "Cinit.h"
#include "Cuuid.h"
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
  BaseThreadPool(poolName, thread), m_port(listenPort), m_spawnListener(listenerOnOwnThread) {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::server::ListenerThreadPool::~ListenerThreadPool() throw() {}


//------------------------------------------------------------------------------
// init
//------------------------------------------------------------------------------
void castor::server::ListenerThreadPool::init() throw (castor::exception::Exception)
{
  /* Create a socket for the server, bind, and listen */
  try {
    sock = new castor::io::ServerSocket(m_port, true);
  }
  catch (castor::exception::Exception e) {
    clog() << "Fatal error: cannot bind socket on port " << m_port << ": "
           << e.getMessage().str() << std::endl;
    throw e;         // calling server should exit() here
  }
}

//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::server::ListenerThreadPool::run()
{  
  if(m_spawnListener) {
    Cthread_create_detached(castor::server::_listener_run, this);
  }
  else {
    listenLoop();
  }
}

//------------------------------------------------------------------------------
// runImpl
//------------------------------------------------------------------------------
void castor::server::ListenerThreadPool::listenLoop()
{
  try {
    for (;;) {
      /* accept connections */
      castor::io::ServerSocket* s = sock->accept();
      /* handle the command */
      threadAssign(s);
    }
  }
  catch(castor::exception::Exception any) {
    clog() << "Error while accepting connections to port " << m_port << ": "
           << any.getMessage().str() << std::endl;
  }
}

//------------------------------------------------------------------------------
// _listener_run
//------------------------------------------------------------------------------
void* castor::server::_listener_run(void* param)
{
  castor::server::ListenerThreadPool* tp = (castor::server::ListenerThreadPool*)param;
  tp->listenLoop();
  return 0;
}

//------------------------------------------------------------------------------
// threadAssign
//------------------------------------------------------------------------------
int castor::server::ListenerThreadPool::threadAssign(void *param)
{
  // Initializing the arguments to pass to the static request processor
  struct threadArgs *args = new threadArgs();
  args->handler = this;
  args->param = param;

  if (m_nbThreads > 0) {   // always true
    int assign_rc = Cpool_assign(m_threadPoolId,
                                 &castor::server::_thread_run,
                                 args,
                                 -1);
    if (assign_rc < 0) {
      clog() << "Error while forking thread in pool " << m_poolName << std::endl;
      return -1;
    }
  } else {
  // for debugging purposes it could be useful to run the user thread code in the same thread. 
    castor::server::_thread_run(args);
  }
  return 0;
}

