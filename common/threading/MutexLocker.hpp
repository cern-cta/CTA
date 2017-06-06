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
#pragma once

#include "Mutex.hpp"
#include "common/exception/Exception.hpp"

#include <pthread.h>

namespace cta { 
namespace threading {

/**
 * Forward declaration of the friend class representing a pthread condition
 * variable.
 */
class CondVar;

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
  MutexLocker(Mutex & m): m_mutex(m) {
    m.lock();
  }
  
  /**
   * Unlocker
   */
  void unlock() {
    if (!m_locked) {
      throw exception::Exception("In MutexLocker::unlock(): trying to unlock an already unlocked mutex");
    }
    m_mutex.unlock();
    m_locked=false;
  }

  /**
   * Locker
   */
  void lock() {
    if (m_locked) {
      throw exception::Exception("In MutexLocker::lock(): trying to relock an locked mutex");
    }
    m_mutex.lock();
    m_locked=true;
  }
  
  /**
   * Destructor.
   */
  ~MutexLocker() {
    if (m_locked) {
      try {
        m_mutex.unlock();
      } catch (...) {
        // Ignore any exceptions
      }
    }
  }

private:
  friend CondVar;

  /**
   * The mutex owened by this MutexLocker.
   */
  Mutex & m_mutex;
  
  /**
   * Tracking of the state of the mutex
   */
  bool m_locked=true;

}; // class MutexLocker
  
} // namespace threading
} // namespace cta
