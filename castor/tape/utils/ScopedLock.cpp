/******************************************************************************
 *                      castor/tape/utils/ScopedLock.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
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
 *
 *
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/tape/utils/ScopedLock.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/serrno.h"

#include <errno.h>
#include <unistd.h>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::utils::ScopedLock::ScopedLock(pthread_mutex_t &mutex)
  throw(castor::exception::Exception) :
  m_mutex(&mutex) {

  const int rc = pthread_mutex_lock(&mutex);

  // Throw an exception if the mutex could not be locked
  if(rc) {
    castor::exception::Exception ex(rc);

    ex.getMessage() <<
      "Failed to lock mutex"
      ": " << sstrerror(rc);

    throw(ex);
  }
}


//-----------------------------------------------------------------------------
// copy constructor
//-----------------------------------------------------------------------------
castor::tape::utils::ScopedLock::ScopedLock(const ScopedLock &s) throw() :
  m_mutex(NULL) {
  
  // This code is never executed because the ScopedLock assignment operator is
  // private
}


//-----------------------------------------------------------------------------
// ScopedLock assignment operator
//-----------------------------------------------------------------------------
castor::tape::utils::ScopedLock
  &castor::tape::utils::ScopedLock::operator=(ScopedLock& obj) throw() {
  
  // This code is never executed because the ScopedLock assignment operator is
  // private
  return *this;
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::utils::ScopedLock::~ScopedLock() throw() {

  if(m_mutex != NULL) {
    pthread_mutex_unlock(m_mutex);
  }
}
