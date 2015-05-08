/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "objectstore/threading/Mutex.hpp"
#include "objectstore/exception/Errnum.hpp"
#include "objectstore/exception/Exception.hpp"

//------------------------------------------------------------------------------
//constructor
//------------------------------------------------------------------------------
cta::threading::Mutex::Mutex()  {
  pthread_mutexattr_t attr;
  cta::exception::Errnum::throwOnReturnedErrno(
    pthread_mutexattr_init(&attr),
    "Error from pthread_mutexattr_init in cta::threading::Mutex::Mutex()");
  cta::exception::Errnum::throwOnReturnedErrno(
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK),
    "Error from pthread_mutexattr_settype in cta::threading::Mutex::Mutex()");
  cta::exception::Errnum::throwOnReturnedErrno(
    pthread_mutex_init(&m_mutex, &attr),
    "Error from pthread_mutex_init in cta::threading::Mutex::Mutex()");
  try {
    cta::exception::Errnum::throwOnReturnedErrno(
      pthread_mutexattr_destroy(&attr),
      "Error from pthread_mutexattr_destroy in cta::threading::Mutex::Mutex()");
  } catch (...) {
    pthread_mutex_destroy(&m_mutex);
    throw;
  }
}
//------------------------------------------------------------------------------
//destructor
//------------------------------------------------------------------------------
cta::threading::Mutex::~Mutex() {
  pthread_mutex_destroy(&m_mutex);
}
//------------------------------------------------------------------------------
//lock
//------------------------------------------------------------------------------
void cta::threading::Mutex::lock()  {
  cta::exception::Errnum::throwOnReturnedErrno(
    pthread_mutex_lock(&m_mutex),
    "Error from pthread_mutex_lock in cta::threading::Mutex::lock()");
}
//------------------------------------------------------------------------------
//unlock
//------------------------------------------------------------------------------
void cta::threading::Mutex::unlock()  {
  cta::exception::Errnum::throwOnReturnedErrno(
  pthread_mutex_unlock(&m_mutex),
          "Error from pthread_mutex_unlock in cta::threading::Mutex::unlock()");
}
