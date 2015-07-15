/******************************************************************************
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/
#pragma once

#include "castor/messages/Mutex.hpp"

#include <pthread.h>
#include <semaphore.h>

namespace castor {
namespace messages {

/**
 * A simple scoped locker for mutexes. Highly recommended as
 * the mutex will be released in all cases (exception, mid-code return, etc...)
 * To use, simply instantiate and forget.
 */
class MutexLocker {
public:

  /**
   * Constructor.
   *
   * @param m pointer to Mutex to be owned.
   */
  MutexLocker(Mutex *const m): m_mutex(m) {
    m->lock();
  }

  /**
   * Destructor.
   */
  ~MutexLocker() throw() {
    try {
      m_mutex->unlock();
    } catch (...) {
      // Ignore any exceptions
    }
  }

private:

  /**
   * The mutex owened by this MutexLocker.
   */
  Mutex *const m_mutex;

}; // class MutexLocker
  
} // namespace messages
} // namespace castor
