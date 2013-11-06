/******************************************************************************
 *                      DbAlertedThreadPool.cpp
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
 * Thread pool supporting wakeup from the DBMS_ALERT Oracle interface
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Include Files
#include <unistd.h>
#include <sys/time.h>

#include "castor/Services.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/server/DbAlertedThreadPool.hpp"

extern "C" {
  char* getconfent (const char *, const char *, int);
}


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::DbAlertedThreadPool::DbAlertedThreadPool(const std::string poolName,
    castor::server::SelectProcessThread* thread,
    const unsigned int nbThreads)
  throw(castor::exception::Exception) :
  BaseThreadPool(poolName, thread, nbThreads)
{
  // Initialize global mutexes
  int rv = pthread_mutex_init(&m_lock, NULL);
  if (rv != 0) {
    castor::exception::Exception e(errno);
    e.getMessage() << "Failed to pthread_mutex_init(m_lock)";
    throw e;
  }

  // Initialize thread attributes
  rv = pthread_attr_init(&m_attr);
  if (rv != 0) {
    castor::exception::Exception e(errno);
    e.getMessage() << "Failed to pthread_attr_init(m_attr)";
    throw e;
  }
  pthread_attr_setdetachstate(&m_attr, PTHREAD_CREATE_DETACHED);
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::server::DbAlertedThreadPool::~DbAlertedThreadPool() throw()
{
  // Destroy global mutexes
  pthread_mutex_destroy(&m_lock);              
  
  // Destroy thread attributes
  pthread_attr_destroy(&m_attr);
}

//------------------------------------------------------------------------------
// shutdown
//------------------------------------------------------------------------------
bool castor::server::DbAlertedThreadPool::shutdown(bool wait) throw()
{
  m_stopped = true;
  if(wait) {
    // Spin lock to make sure no thread is still active
    while(m_nbActiveThreads > 0) {
      usleep(100000);
    }
  }
  return (m_nbActiveThreads == 0);
}


//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::server::DbAlertedThreadPool::run()
  throw (castor::exception::Exception)
{
  if(m_nbThreads == 0) {
    return;
  }
  pthread_t t;
  int       rv;
  
  // create pool of detached threads
  for (unsigned i = 0; i < m_nbThreads; i++) {
    rv = pthread_create(&t, &m_attr, (void *(*)(void *))&DbAlertedThreadPool::_runner, this);
    if (rv != 0) {
      break;
    }
  }
  if (rv != 0) {
    m_stopped = true;
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Failed to create pool " << m_poolName;
    throw ex;
  }
  else {
    // "Thread pool started"
    castor::dlf::Param params[] =
      {castor::dlf::Param("ThreadPool", m_poolName),
       castor::dlf::Param("Type", "DbAlertedThreadPool"),
       castor::dlf::Param("NbThreads", m_nbThreads)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,
                            DLF_BASE_FRAMEWORK + 3, 3, params);
  }
}


//------------------------------------------------------------------------------
// _runner
//------------------------------------------------------------------------------
void* castor::server::DbAlertedThreadPool::_runner(void* param)
{
  DbAlertedThreadPool* pool = (DbAlertedThreadPool*)param;
  SelectProcessThread* thread = dynamic_cast<SelectProcessThread*>(pool->m_thread);
  timeval tv1, tv2;
  double activeTime = 0, idleTime = 0;
  gettimeofday(&tv2, NULL);

  try {
    // Thread initialization
    thread->init();

    while (!pool->m_stopped) {
      // wait to be woken up by an alert or for a timeout
      castor::IObject* obj = thread->select();

      // we may have been stopped while sleeping
      if(!pool->m_stopped && 0 != obj) {

        // reset errno and serrno
        errno = 0;
        serrno = 0;

        // do the user job and catch any exception for logging purposes
        try {
          gettimeofday(&tv1, NULL);
          idleTime = tv1.tv_sec - tv2.tv_sec + (tv1.tv_usec - tv2.tv_usec)/1000000.0;

          // we pass the pool itself as parameter to allow e.g.
          // SelectProcessThreads to call our stopped() method
          thread->process(obj);
          
          gettimeofday(&tv2, NULL);
          activeTime = tv2.tv_sec - tv1.tv_sec + (tv2.tv_usec - tv1.tv_usec)/1000000.0;

          // "Task processed"         
          castor::dlf::Param params[] =
            {castor::dlf::Param("ThreadPool", pool->m_poolName),
             castor::dlf::Param("ProcessingTime", activeTime)};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                                  DLF_BASE_FRAMEWORK + 22, 2, params);
        }
        catch (castor::exception::Exception& e) {
          // "Exception caught in the user thread"
          castor::dlf::Param params[] =
            {castor::dlf::Param("Error", sstrerror(e.code())),
             castor::dlf::Param("Message", e.getMessage().str())};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
                                  DLF_BASE_FRAMEWORK + 4, 2, params);
        }
      }

      // we are not anymore a running service
      pthread_mutex_lock(&pool->m_lock);
      pool->m_nbActiveThreads--;
      // update shared timers
      pool->m_activeTime += activeTime;
      pool->m_idleTime += idleTime;
      pool->m_runsCount++;
      pthread_mutex_unlock(&pool->m_lock);

      // and continue forever until shutdown() is called
    }

    // notify the user thread that we are over
    thread->stop();
  }
  catch (castor::exception::Exception& any) {
    try {
      pool->m_thread->stop();
      pthread_mutex_unlock(&pool->m_lock);
    }
    catch(...) {}

    // "Thread run error"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Error", sstrerror(any.code())),
       castor::dlf::Param("Message", any.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
                            DLF_BASE_FRAMEWORK + 5, 2, params);
  }

  try {
    // cleanup thread specific globals
    delete (services());
  } catch (...) {
    // ignore errors
  }
  return 0;
}
