/******************************************************************************
 *                      BaseThreadPool.cpp
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
 * @(#)$RCSfile: BaseThreadPool.cpp,v $ $Revision: 1.17 $ $Release$ $Date: 2008/10/02 08:09:07 $ $Author: itglp $
 *
 * Abstract CASTOR thread pool
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Include Files
#include <signal.h>
#include "castor/server/BaseThreadPool.hpp"
#include "castor/MsgSvc.hpp"
#include "castor/Services.hpp"
#include "castor/exception/Internal.hpp"
#include "Cinit.h"
#include "Cuuid.h"
#include "Cpool_api.h"
#include "castor/logstream.h"
#include <sstream>
#include <iomanip>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::BaseThreadPool::BaseThreadPool(const std::string poolName,
                                               castor::server::IThread* thread) :
  BaseObject(), m_threadPoolId(-1),
  m_nbThreads(castor::server::DEFAULT_THREAD_NUMBER),
  m_poolName(poolName), m_thread(thread) {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::server::BaseThreadPool::~BaseThreadPool() throw()
{
  shutdown();
  if(m_thread != 0) {
    delete m_thread;
  }
}

//------------------------------------------------------------------------------
// init
//------------------------------------------------------------------------------
void castor::server::BaseThreadPool::init() throw (castor::exception::Exception)
{
  castor::exception::Internal notImpl;
  notImpl.getMessage() << 
    "BaseThreadPool is (pseudo)abstract, you must use its derived classes";
  throw notImpl;
}

//------------------------------------------------------------------------------
// shutdown
//------------------------------------------------------------------------------
bool castor::server::BaseThreadPool::shutdown() throw()
{
  if(m_thread != 0) {
    try {
      m_thread->stop();
    } catch(castor::exception::Exception ignored) {}
  }
  return true;
}

//------------------------------------------------------------------------------
// setNbThreads
//------------------------------------------------------------------------------
void castor::server::BaseThreadPool::setNbThreads(int value)
{
  m_nbThreads = value;
}

//------------------------------------------------------------------------------
// join XXX to be implemented later
//------------------------------------------------------------------------------
//void castor::server::BaseThreadPool::join(int tid)
//{
//  Cthread_join(tid, NULL);
//}


//------------------------------------------------------------------------------
// _thread_run
//------------------------------------------------------------------------------
void* castor::server::_thread_run(void* param)
{
  castor::server::BaseThreadPool* pool = 0;
  struct threadArgs *args;

  if (param == NULL) {
    return 0;
  }

  args = (struct threadArgs*)param;
  pool = dynamic_cast<castor::server::BaseThreadPool*>(args->handler);
  if (pool == 0 || pool->m_thread == 0) {
    delete args;
    return 0;
  }

  // Executes the thread
  try {
    pool->m_thread->run(args->param);
  } catch(castor::exception::Exception any) {
    pool->clog() << ERROR << "Uncaught exception in a thread from pool "
                 << pool->m_poolName << " : " << any.getMessage().str() << std::endl;
  } catch(...) {
    pool->clog() << ERROR << "Uncaught GENERAL exception in a thread from pool "
                 << pool->m_poolName << std::endl;
  }
  
  delete args;
  return 0;
}

