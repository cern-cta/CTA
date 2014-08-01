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

#pragma once

#include <pthread.h>

namespace castor {
namespace utils {

/**
 * A simple scoped-lock on a mutex.  When the scoped-lock goes out of scope,
 * it will unlock the mutex.
 */
class ScopedLock {

public:

  /**
   * Constructor.
   *
   * Takes a lock on the specified mutex.
   *
   * @param mutex The mutex on which the lock should be taken.
   */
  ScopedLock(pthread_mutex_t &mutex);

  /**
   * Destructor.
   *
   * Unlocks the mutex.
   */
  ~ScopedLock() throw();

private:

  /**
   * Private and not implemented copy-constructor to prevent users from trying
   * to create a new copy of an object of this class.
   */
  ScopedLock(const ScopedLock &s) throw();

  /**
   * Private and not implemented assignment-operator to prevent users from
   * trying to assign one object of this class to another.
   */
  ScopedLock &operator=(ScopedLock& obj) throw();

  /**
   * The mutex on which the lock has been taken.
   */
  pthread_mutex_t *m_mutex;

}; // class ScopedLock

} // namespace utils
} // namespace castor
