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
 * @(#)$RCSfile: SignalThreadPool.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2005/11/28 09:42:51 $ $Author: itglp $
 *
 *
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Include Files
#include "castor/server/SignalThreadPool.hpp"
#include "castor/server/ServiceThread.hpp"
#include "castor/exception/Internal.hpp"


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::SignalThreadPool::SignalThreadPool(const std::string poolName,
                                 castor::server::IThread* thread,
                                 const int nbThreads,
                                 const int notifyPort) throw() :
  BaseThreadPool(poolName, new castor::server::ServiceThread(thread, 0), nbThreads),   // XXX timeout = 0 by default -> check consistency with mutex timeout...
  m_notifyPort(notifyPort) {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::server::SignalThreadPool::~SignalThreadPool() throw()
{
  //delete m_notifThread;
  delete m_poolMutex;
}


//------------------------------------------------------------------------------
// init
//------------------------------------------------------------------------------
void castor::server::SignalThreadPool::init()
  throw (castor::exception::Exception)
{
  /* Initialize service api (it is important to do that only after the signal mask creation) */
  /* ======================================================================================= */

  m_nbActiveThreads = 0;  /* Number of threads currently running that service */
  m_nbTotalThreads = 0;   /* Number of threads currently running that service */
  m_notified = 0;         /* By default no signal yet has been received */
  m_nbNotifyThreads = 0;  /* Maximum one notification thread should run -- XXX this var can assume values in {-1,0,1} -> move to enum */
  m_notTheFirstTime = false;

  /* Create a mutex (could throw exception) */
  m_poolMutex = new Mutex(-1);
}


//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::server::SignalThreadPool::run()
{
  // create pool of detached threads
  int n = 0;
  for (int i = 0; i < m_nbTotalThreads; i++) {
    if (Cthread_create_detached(castor::server::_thread_run,this) >= 0) {
      ++n;
    }
  }
  if (n <= 0) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to create pool '" << m_poolName;
    throw ex;
  }
  // create notification thread
  //m_notifThread = new NotificationThread(this);
  //...
  // now the detached threads run the old service_runService
}



//------------------------------------------------------------------------------
// commitRun
//------------------------------------------------------------------------------
void castor::server::SignalThreadPool::commitRun()
  throw (castor::exception::Exception)
{
  // get lock on the shared mutex
  m_poolMutex->lock();

  /* Try to start the notification thread */
  /* XXX not supported for now */
  //if (singleService.nbNotifyThreads == 0) {

    /* The following is a trick to make sure that only one service thread is waiting on the notification thread's notification! */
    //singleService.nbNotifyThreads = -1;

    //if (Cthread_create_detached(single_service_notifyThread,NULL) < 0) {
    //  rc = -1;
    //  goto single_service_serviceCleanupAndReturn;
    //}

    /* Wait for nbNotifyThreads to change */
    //while (singleService.nbNotifyThreads != 1) {
    //  Cthread_cond_timedwait_ext(single_service_serviceLockCthreadStructure,SINGLE_SERVICE_NOTIFICATION_TIMEOUT);
    //}

  //}

  /* At startup we assume that a service can start at least once right /now/ */
  if (!m_notTheFirstTime) {
    m_notified++; /* Hack to not pass in the mutex/cond loop the 1st time */
    m_notTheFirstTime = true;
  }

  m_nbTotalThreads++;

  /* Unlock the mutex */
  try {
    m_poolMutex->release();
  }
  catch (castor::exception::Exception e) {
    m_nbTotalThreads--;
    throw e;
  }
}


//------------------------------------------------------------------------------
// waitSignalOrTimeout
//------------------------------------------------------------------------------
void castor::server::SignalThreadPool::waitSignalOrTimeout()
  throw (castor::exception::Exception)
{
  /* Wait for a notification or a timeout, unless we were already notified */
  if (m_notified == 0) {
    m_poolMutex->wait();
  }

  /* We can be here because: */
  /* - we were notified */
  /* - we got timeout */
  /* - waiting on condition variable got interrupted */
  /* - the very first time that service is running, singleService.notified is already 1, bypassing the wait */

  /* Notify that we are a running service */
  m_nbActiveThreads++;

  /* In case we were waked up because of a notification, make sure we reset the notification flag */
  if (m_notified > 0) {
    m_notified--;
  }

  /* Unlock the mutex */
  try {
    m_poolMutex->release();
  }
  catch (castor::exception::Exception e) {
    m_nbActiveThreads--;
    throw e;
  }
}


//------------------------------------------------------------------------------
// commitRelease
//------------------------------------------------------------------------------
void castor::server::SignalThreadPool::commitRelease()
{
  /* Re-aquire the mutex */
  try {
    m_poolMutex->lock();
    m_nbActiveThreads--;
  }
  catch (castor::exception::Exception e) {
    m_nbActiveThreads--;   // unsafe
    throw e;
  }
}

