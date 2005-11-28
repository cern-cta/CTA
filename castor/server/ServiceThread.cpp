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
 * @(#)$RCSfile: ServiceThread.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2005/11/28 09:42:51 $ $Author: itglp $
 *
 *
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Include Files
#include "castor/server/ServiceThread.hpp"
#include "castor/exception/Internal.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::ServiceThread::ServiceThread(IThread* userThread, int timeout) :
  m_userThread(userThread), m_owner(0), m_timeout(timeout)
{
};

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::server::ServiceThread::~ServiceThread() throw()
{
  delete m_userThread;
}

//------------------------------------------------------------------------------
// init
//------------------------------------------------------------------------------
void castor::server::ServiceThread::init(void* param)
{
  m_owner = (SignalThreadPool*)param;
  if(m_userThread)
    m_userThread->init(m_owner);
};

//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::server::ServiceThread::run()
{
  if (m_userThread == 0) {
    serrno = EINVAL;
    return;
  }

  try {

    /* Notify the pool that we are starting */
    m_owner->commitRun();

    while (true) {

      /* wait to be woken up by a signal or for a timeout */
      m_owner->waitSignalOrTimeout();

      /* Reset errno and serrno */
      errno = 0;
      serrno = 0;

      /* Do the user job */
      m_userThread->run();

      /* Notify that are not anymore a running service */
      m_owner->commitRelease();

      /* And continue */
    }

    //owner->nbTotalThreads--;   XXX we can't reach this point!

  }
  catch (castor::exception::Exception any) {
    try {
      m_owner->getMutex()->release();
    } catch(...) {}
    // LOG
  }
}

