/******************************************************************************
 *                      ServiceThread.cpp
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
 * @(#)$RCSfile: ServiceThread.cpp,v $ $Revision: 1.12 $ $Release$ $Date: 2008/05/30 14:08:25 $ $Author: itglp $
 *
 * Internal thread to allow user service threads running forever
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#include "castor/server/ServiceThread.hpp"
#include "castor/exception/Internal.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::ServiceThread::ServiceThread(IThread* userThread) :
  m_owner(0), m_stopped(false), m_userThread(userThread) {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::server::ServiceThread::~ServiceThread() throw() {
  if(m_userThread) {
    delete m_userThread;
    m_userThread = 0;
  }
}

//------------------------------------------------------------------------------
// stop
//------------------------------------------------------------------------------
void castor::server::ServiceThread::stop() {
  if(m_stopped) {
    // Already stopped: this means that the user thread didn't terminate
    // yet the run() method. Let's try to tell it to stop directly.
    m_userThread->stop();
  }
  m_stopped = true;
}

//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::server::ServiceThread::run(void* param) {
  if (m_userThread == 0) {
    serrno = EINVAL;
    return;
  }

  try {
    m_owner = (SignalThreadPool*)param;

    // Notify the pool that we are starting and perform user initialization
    m_owner->commitRun();
    m_userThread->init();
    
    while (!m_stopped) {
      // wait to be woken up by a signal or for a timeout
      m_owner->waitSignalOrTimeout();

      // reset errno and serrno
      errno = 0;
      serrno = 0;

      // do the user job: catch any exception for logging purposes
      try {
        // we pass ourselves as parameter to allow e.g.
        // SelectProcessThreads to call our stopped() method
        m_userThread->run(this);
      } catch (castor::exception::Exception e) {
        m_owner->clog() << ERROR << "Exception caught in the user thread: "
          << e.getMessage().str() << std::endl;
      }

      // notify that we are not anymore a running service
      m_owner->commitRelease();

      // and continue forever until a SIGTERM is caught
    }
    
    // notify the user thread that we are over,
    // e.g. for dropping a db connection
    m_userThread->stop();
  }
  catch (castor::exception::Exception any) {
    try {
      m_owner->getMutex()->release();
    } catch(...) {}
    m_owner->clog() << ERROR << "Thread run error: "
      << any.getMessage().str() << std::endl;
  }
}

