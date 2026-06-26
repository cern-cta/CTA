/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-FileCopyrightText: 2026 DESY
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "Mutex.hpp"
#include "common/exception/Exception.hpp"

namespace cta::threading {

/**
 * Forward declaration of the friend class representing a condition variable.
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
   * @param m reference to Mutex to be locked
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
    m_locked = false;
  }

  /**
   * Locker
   */
  void lock() {
    if (m_locked) {
      throw exception::Exception("In MutexLocker::lock(): trying to relock a locked mutex");
    }
    m_mutex.lock();
    m_locked = true;
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
   * The mutex owned by this MutexLocker.
   */
  Mutex& m_mutex;

  /**
   * Tracking of the state of the mutex
   */
  bool m_locked = true;

};  // class MutexLocker

}  // namespace cta::threading
