/******************************************************************************
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/exception/Exception.hpp"
#include "castor/utils/ScopedLock.hpp"
#include "h/serrno.h"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::utils::ScopedLock::ScopedLock(pthread_mutex_t &mutex): m_mutex(&mutex) {
  const int rc = pthread_mutex_lock(&mutex);
  if(0 != rc) {
    char message[100];
    sstrerror_r(rc, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create scoped lock"
      ": Failed to lock mutex: " << message;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::utils::ScopedLock::~ScopedLock() throw() {
  if(m_mutex != NULL) {
    pthread_mutex_unlock(m_mutex);
  }
}
