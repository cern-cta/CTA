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
 * @(#)$RCSfile: SignalThreadPool.cpp,v $ $Revision: 1.11 $ $Release$ $Date: 2006/08/14 19:13:06 $ $Author: itglp $
 *
 *
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Include Files
#include "castor/server/SignalThreadPool.hpp"
#include "castor/server/ServiceThread.hpp"
#include "castor/exception/Internal.hpp"

extern "C" {
  char* getconfent (const char *, const char *, int);
}


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::SignalThreadPool::SignalThreadPool(const std::string poolName,
                                 castor::server::IThread* thread,
                                 const int notifPort,
                                 const int timeout) :
  BaseThreadPool(poolName, new castor::server::ServiceThread(thread)),
  m_notifPort(notifPort), m_timeout(timeout) {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::server::SignalThreadPool::~SignalThreadPool() throw()
{
  if(m_notifTPool)
    delete m_notifTPool;
  delete m_poolMutex;
}


//------------------------------------------------------------------------------
// init
//------------------------------------------------------------------------------
void castor::server::SignalThreadPool::init()
  throw (castor::exception::Exception)
{
  // Initialize shared variables (former singleService structure)
  m_nbActiveThreads = 0;  /* Number of threads currently running the service */
  m_nbTotalThreads = 0;   /* Total number of threads for this service */
  m_notified = 0;         /* By default no signal yet has been received */
  m_notTheFirstTime = false;

  // Create a mutex (could throw exception)
  m_poolMutex = new Mutex(-1, m_timeout);
}


//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::server::SignalThreadPool::run()
{
  // create pool of detached threads
  int n = 0;
  struct threadArgs *args = new threadArgs();
  args->handler = this;
  args->param = this;

  // even if nbThreads = 1 it will run detached because we
  // always run the notification and the signal handler thread.
  for (int i = 0; i < m_nbThreads; i++) {
    if (Cthread_create_detached(castor::server::_thread_run, args) >= 0) {
      ++n;
    }
  }
  if (n <= 0) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to create pool " << m_poolName;
    throw ex;
  }
  else {
    m_nbThreads = n;
    clog() << DEBUG << "Thread pool " << m_poolName << " started with "
           << m_nbThreads << " threads" << std::endl;
  }

  /* create and start notification thread if requested */
  m_notifTPool = 0;
  if(m_notifPort <= 0)
    return;

  m_notifTPool = new BaseThreadPool("_Notif_" + m_poolName, new NotificationThread(m_notifPort));
  struct threadArgs *nArgs = new threadArgs();
  nArgs->handler = m_notifTPool;
  nArgs->param = this;          // reuse BaseThreadPool only for the thread entrypoint

  if(Cthread_create_detached(castor::server::_thread_run, nArgs) < 0) {
    delete nArgs;
    delete m_notifTPool;
    m_notifTPool = 0;
    return;
  }

  /* here we had a wait loop for nbNotifyThreads to change - not needed anymore */
}


//------------------------------------------------------------------------------
// commitRun
//------------------------------------------------------------------------------
void castor::server::SignalThreadPool::commitRun()
  throw (castor::exception::Exception)
{
  // get lock on the shared mutex
  m_poolMutex->lock();

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
  /* - the very first time that service is running, m_notified is already 1, bypassing the wait */

  /* In case we were waked up because of a notification, make sure we reset the notification flag */
  if (m_notified > 0) {
    m_notified--;
  }

  /* Notify that the calling thread is a running service */
  m_nbActiveThreads++;

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
  try {
    /* The calling thread is not anymore a running service */
    m_poolMutex->lock();
    m_nbActiveThreads--;
  }
  catch (castor::exception::Exception e) {
    m_nbActiveThreads--;   // unsafe
    throw e;
  }
}


//------------------------------------------------------------------------------
// getNotifPort - deprecated
//------------------------------------------------------------------------------
//int castor::server::SignalThreadPool::getNotifPort()
//{
//  if(m_notifPort > 0)
//    return m_notifPort;

//  /* Try to get port if not provided */
//  /* - From environment variable */
//  std::string varName = getPoolId() + NOTIFY_PORT;
//  std::string envName = "DAEMON_" + varName;
//  char *port = NULL;
//  if ((port = getenv(envName.c_str())) != NULL) {
//    m_notifPort = atoi(port);
//  } else {
//    /* - From configuration file */
//    if ((port = getconfent("DAEMON", varName.c_str(), 0)) != NULL) {
//      m_notifPort = atoi(port);
//    } else {
//      /* - (obsolete) from /etc/services or NIS */
//      struct servent *sp = NULL;
//      if ((sp = Cgetservbyname(m_poolName.c_str(), NOTIFY_PROTO)) != NULL) {
//        m_notifPort = ntohs(sp->s_port);
//     } else {
//    	  /* - Default value: use the ASCII value of the pool id */
//        m_notifPort = NOTIFY_PORT_BASE + (int)getPoolId();
//      }
//    }
//  }
//  if (m_notifPort <= 0) {
//    /* A port must be > 0 */
//    serrno = EINVAL;
//  }
//  return m_notifPort;
//}

