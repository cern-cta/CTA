/******************************************************************************
 *                      SignalThreadPool.cpp
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
 * @(#)$RCSfile: SignalThreadPool.cpp,v $ $Revision: 1.25 $ $Release$ $Date: 2009/08/11 10:34:43 $ $Author: itglp $
 *
 * Thread pool supporting wakeup on signals and periodical run after timeout
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Include Files
#include "castor/server/SignalThreadPool.hpp"
#include "castor/exception/Internal.hpp"
#include <sys/time.h>

extern "C" {
  char* getconfent (const char *, const char *, int);
}


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::SignalThreadPool::SignalThreadPool(const std::string poolName,
                                                   castor::server::IThread* thread,
                                                   const int timeout,
                                                   const unsigned int nbThreads,
                                                   const unsigned int startingThreads)
  throw(castor::exception::Exception) :
  BaseThreadPool(poolName, thread, nbThreads),
  m_poolMutex(-1, (unsigned)timeout), m_notified(startingThreads)
{}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::server::SignalThreadPool::~SignalThreadPool() throw()
{}

//------------------------------------------------------------------------------
// shutdown
//------------------------------------------------------------------------------
bool castor::server::SignalThreadPool::shutdown(bool wait) throw()
{
  try {
    if(m_stopped) {
      // quick answer if we were already told to stop
      return (m_notified + m_nbActiveThreads == 0);
    }
    m_poolMutex.lock();
    m_stopped = true;
    // notify all idle threads so that they can properly shutdown
    m_notified = m_nbThreads - m_nbActiveThreads;
    m_poolMutex.signal();
    m_poolMutex.release();
    if(wait) {
      // Spin lock to make sure no thread is still active
      while(m_notified > 0) {
        usleep(100000);
      }
    }
    return (m_notified + m_nbActiveThreads == 0);
  }
  catch (castor::exception::Exception e) {
    // This can happen in case of mutex problems.
    // We just try again at the next round.
    try {
      m_poolMutex.release();
    } catch(castor::exception::Exception ignored) {};
    return false;
  }
}


//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::server::SignalThreadPool::run()
  throw (castor::exception::Exception)
{
  if(m_nbThreads == 0) {
    return;
  }
  unsigned int n = 0;

  // create pool of detached threads
  for (unsigned i = 0; i < m_nbThreads; i++) {
    if (Cthread_create_detached(
          (void *(*)(void *))&SignalThreadPool::_runner, this) >= 0) {
      ++n;
    }
  }
  if (n == 0) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to create pool " << m_poolName;
    throw ex;
  }
  else {
    m_nbThreads = n;
    if(m_notified > (int)n) {
      m_notified = n;
    }
    // "Thread pool started"
    castor::dlf::Param params[] =
      {castor::dlf::Param("ThreadPool", m_poolName),
       castor::dlf::Param("Type", "SignalThreadPool"),
       castor::dlf::Param("NbThreads", m_nbThreads)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,
                            DLF_BASE_FRAMEWORK + 3, 3, params);
  }
}


//------------------------------------------------------------------------------
// waitSignalOrTimeout
//------------------------------------------------------------------------------
void castor::server::SignalThreadPool::waitSignalOrTimeout()
  throw (castor::exception::Exception)
{
  m_poolMutex.lock();

  // Check if we were notified
  if (m_notified > 0) {
    m_notified--;
  }
  else {
    // in the general case we wait for a notification or timeout
    m_poolMutex.release();
    m_poolMutex.wait();
    // check again if at this point we were notified
    if(m_notified > 0) {
      m_notified--;
    }
  }
  // We can be here either because we were notified or for a timeout;
  // in any case we are now a running thread, update internal counter
  m_nbActiveThreads++;
  m_poolMutex.release();
}


//------------------------------------------------------------------------------
// _runner
//------------------------------------------------------------------------------
void* castor::server::SignalThreadPool::_runner(void* param)
{
  SignalThreadPool* pool = (SignalThreadPool*)param;
  timeval tv1, tv2;
  double activeTime = 0, idleTime = 0;
  gettimeofday(&tv2, NULL);

  try {
    // Thread initialization
    pool->m_thread->init();

    while (!pool->m_stopped) {
      // wait to be woken up by a signal or for a timeout
      pool->waitSignalOrTimeout();

      // we may have been stopped while sleeping
      if(!pool->m_stopped) {

        // reset errno and serrno
        errno = 0;
        serrno = 0;

        // do the user job and catch any exception for logging purposes
        try {
          gettimeofday(&tv1, NULL);
          idleTime = tv1.tv_sec - tv2.tv_sec + (tv1.tv_usec - tv2.tv_usec)/1000000.0;

          // we pass the pool itself as parameter to allow e.g.
          // SelectProcessThreads to call our stopped() method
          pool->m_thread->run(pool);
          
          gettimeofday(&tv2, NULL);
          activeTime = tv2.tv_sec - tv1.tv_sec + (tv2.tv_usec - tv1.tv_usec)/1000000.0;

          // "Task processed"         
          castor::dlf::Param params[] =
            {castor::dlf::Param("ThreadPool", pool->m_poolName),
             castor::dlf::Param("ProcessingTime", activeTime)};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                                  DLF_BASE_FRAMEWORK + 22, 2, params);
        }
        catch (castor::exception::Exception e) {
          // "Exception caught in the user thread"
          castor::dlf::Param params[] =
            {castor::dlf::Param("Error", sstrerror(e.code())),
             castor::dlf::Param("Message", e.getMessage().str())};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
                                  DLF_BASE_FRAMEWORK + 4, 2, params);
        }
      }

      // we are not anymore a running service
      try {
        pool->m_poolMutex.lock();
        pool->m_nbActiveThreads--;
        // update shared timers
        pool->m_activeTime += activeTime;
        pool->m_idleTime += idleTime;
        pool->m_runsCount++;
      }
      catch (castor::exception::Exception e) {
        pool->m_nbActiveThreads--;   // unsafe
        throw e;
      }
      pool->m_poolMutex.release();

      // and continue forever until shutdown() is called
    }

    // notify the user thread that we are over,
    // e.g. for dropping a db connection
    pool->m_thread->stop();
  }
  catch (castor::exception::Exception any) {
    try {
      pool->m_thread->stop();
      pool->m_poolMutex.release();
    }
    catch(...) {}

    // "Thread run error"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Error", sstrerror(any.code())),
       castor::dlf::Param("Message", any.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
                            DLF_BASE_FRAMEWORK + 5, 2, params);
  }

  return 0;
}
