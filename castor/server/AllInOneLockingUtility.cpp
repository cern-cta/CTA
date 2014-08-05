/******************************************************************************
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
 * C++ interface to mutex handling with the Cthread API
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include <signal.h>
#include "castor/server/AllInOneLockingUtility.hpp"
#include "Cthread_api.h"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::AllInOneLockingUtility::AllInOneLockingUtility(int value, unsigned int timeout)
   :
  m_var(value), m_timeout(timeout), m_mutexCthread(0)
{
  if(createLock() != 0) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create mutex";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::server::AllInOneLockingUtility::~AllInOneLockingUtility()
{
  Cthread_mutex_unlock_ext(m_mutexCthread);
  // Cthread_mutex_destroy(m_mutexCthread);  // XXX this causes a deadlock!
  // XXX to be eventually replaced by standard pthread mutexes
}

//------------------------------------------------------------------------------
// createLock
//------------------------------------------------------------------------------
int castor::server::AllInOneLockingUtility::createLock()
{
  if(Cthread_mutex_timedlock(&m_var, m_timeout) != 0) {
  	return -1;
  }
  m_mutexCthread = Cthread_mutex_lock_addr(&m_var);
  if(m_mutexCthread == 0) {
    Cthread_mutex_unlock(&m_var);
    return -1;
  }
  if(Cthread_mutex_unlock_ext(m_mutexCthread) != 0) {
  	return -1;
  }
  return 0;
}

//------------------------------------------------------------------------------
// setValue
//------------------------------------------------------------------------------
void castor::server::AllInOneLockingUtility::setValue(int newValue)
  
{
  int oldValue = m_var;
  lock();
  m_var = newValue;
  try {
    release();
  } catch(castor::exception::Exception& e) {
    m_var = oldValue;
    throw e;
  }
}

//------------------------------------------------------------------------------
// setValueNoEx
//------------------------------------------------------------------------------
void castor::server::AllInOneLockingUtility::setValueNoEx(int newValue)
{
  if (Cthread_mutex_timedlock_ext(m_mutexCthread, m_timeout) == 0) {
    m_var = newValue;
    Cthread_mutex_unlock_ext(m_mutexCthread);
  }
  else {   // thread-unsafe
    m_var = newValue;
  }
}


//------------------------------------------------------------------------------
// wait
//------------------------------------------------------------------------------
void castor::server::AllInOneLockingUtility::wait()
{
  if (m_mutexCthread == 0)
    return;
  Cthread_cond_timedwait_ext(m_mutexCthread, m_timeout);
  // here the timeout was previously defined as COND_TIMEOUT, by default = TIMEOUT
}

//------------------------------------------------------------------------------
// lock
//------------------------------------------------------------------------------
void castor::server::AllInOneLockingUtility::lock() 
{
  if (m_mutexCthread == 0 ||
      Cthread_mutex_timedlock_ext(m_mutexCthread, m_timeout) != 0) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to lock mutex";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// release
//------------------------------------------------------------------------------
void castor::server::AllInOneLockingUtility::release() 
{
  if(Cthread_mutex_unlock_ext(m_mutexCthread) != 0) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to release mutex";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// signal
//------------------------------------------------------------------------------
void castor::server::AllInOneLockingUtility::signal() 
{
  if (m_mutexCthread == 0 ||
      Cthread_cond_signal_ext(m_mutexCthread) != 0) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to signal mutex";
    throw ex;
  }
}


