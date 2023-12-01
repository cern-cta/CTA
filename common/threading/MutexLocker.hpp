/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */
#pragma once

#include "Mutex.hpp"
#include "common/exception/Exception.hpp"

#include <pthread.h>

namespace cta::threading {

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
   * Constructor
   *
   * @param m pointer to Mutex to be owned
   */
  explicit MutexLocker(Mutex& m) : m_mutex(m) { m.lock(); }
  
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
  
} // namespace cta::threading
