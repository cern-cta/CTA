/******************************************************************************
 *                      NotifierThread.cpp
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
 * @(#)$RCSfile: NotifierThread.cpp,v $ $Revision: 1.7 $ $Release$ $Date: 2009/07/13 06:22:07 $ $Author: waldron $
 *
 * A thread to handle notifications to wake up workers in a pool
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Include Files
#include "castor/server/NotifierThread.hpp"
#include "castor/server/ThreadNotification.hpp"
#include "castor/server/SignalThreadPool.hpp"

// Initialization of the singleton
castor::server::NotifierThread* castor::server::NotifierThread::s_Instance(0);

//------------------------------------------------------------------------------
// getInstance
//------------------------------------------------------------------------------
castor::server::NotifierThread* castor::server::NotifierThread::getInstance
(castor::server::BaseDaemon* owner) {
  if (0 == s_Instance && 0 != owner) {
    // The singleton is created only when the owner parameter is supplied;
    // otherwise, getInstance(0) may return 0.
    // Note that we are not protecting the instantiation with a mutex
    // because this class is instantiated before spawning any thread
    s_Instance = new castor::server::NotifierThread(owner);
  }
  return s_Instance;
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::NotifierThread::NotifierThread(castor::server::BaseDaemon* owner) :
  m_owner(owner) {}

//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::server::NotifierThread::run(void* param) {
  // get the object
  castor::server::ThreadNotification* notif =
    (castor::server::ThreadNotification*)param;

  // do the actual job
  doNotify(notif->tpName(), notif->nbThreads());

  delete notif;
}

//------------------------------------------------------------------------------
// doNotify
//------------------------------------------------------------------------------
void castor::server::NotifierThread::doNotify(char tpName, int nbThreads) throw () {
  castor::server::SignalThreadPool* pool = 0;
  try {
    // first resolve the pool
    pool = dynamic_cast<castor::server::SignalThreadPool*>
      (m_owner->getThreadPool(tpName));
    if(pool == 0)
      // only SignalThreadPool's are valid
      return;

    // lock the condition variable: we are friend of the SignalThreadPool
    pool->m_poolMutex.lock();

    pool->m_notified += nbThreads;

    // We make sure that 0 <= m_notified <= nbThreadInactive
    if(pool->m_notified < 0) {
      pool->m_notified = 1;
    }
    int nbThreadInactive = pool->m_nbThreads - pool->m_nbActiveThreads;
    if(nbThreadInactive < 0) {
      nbThreadInactive = 0;
    }
    if (nbThreadInactive == 0) {
      // All threads are already busy : try to get one counting on timing windows
      pool->m_notified = 1;
    } else {
      if (pool->m_notified > nbThreadInactive) {
        pool->m_notified = nbThreadInactive;
      }
    }

    if (pool->m_notified > 0) {
      // wake up sleeping thread(s)
      pool->m_poolMutex.signal();
    }

    pool->m_poolMutex.release();
  }
  catch (castor::exception::Exception& any) {
    // just ignore for this loop all mutex errors and try again
    if(pool) {
      // "NotifierThread exception"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Error", sstrerror(any.code())),
         castor::dlf::Param("Message", any.getMessage().str())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING,
                              DLF_BASE_FRAMEWORK + 6, 2, params);
      try {
        pool->m_poolMutex.release();
      } catch(...) {}
    }
  }
}
