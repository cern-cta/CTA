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
* @(#)$RCSfile: NotifierThread.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2007/11/26 13:17:23 $ $Author: waldron $
*
* A thread to handle notifications to wake up workers in a pool
*
* @author Giuseppe Lo Presti
*****************************************************************************/

// Include Files
#include "castor/server/NotifierThread.hpp"
#include "castor/server/ThreadNotification.hpp"
#include "castor/server/SignalThreadPool.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::NotifierThread::NotifierThread(castor::server::BaseDaemon* owner) :
  m_owner(owner) {}

//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::server::NotifierThread::run(void* param) {
  castor::server::SignalThreadPool* pool = 0; 
  try {
    // get the object
    castor::server::ThreadNotification* notif =
      (castor::server::ThreadNotification*)param;

    // we have received a notification, first resolve the pool
    pool = dynamic_cast<castor::server::SignalThreadPool*>
      (m_owner->getThreadPool(notif->tpName()));
    if(pool == 0)
      // only SignalThreadPool are valid
      return;
    
    // then signal the condition variable: we are friend of the SignalThreadPool
    pool->m_poolMutex->lock();
    
    pool->m_notified += notif->nbThreads();
    
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
      pool->m_poolMutex->signal();
    }
    
    pool->m_poolMutex->release();
  }
  catch (castor::exception::Exception any) {
    // just ignore for this loop all mutex errors and try again
    if(pool) {
      pool->clog() << WARNING << any.getMessage().str() << std::endl;
      try {
        pool->m_poolMutex->release();
      } catch(...) {}
    }
  }
}
