/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <pthread.h>
#include <semaphore.h>

namespace cta::threading {

/**
 * Forward declaration of the friend class that represents a pthread condition
 * variable.
 */
class CondVar;

/**
* A simple exception throwing wrapper for pthread mutexes.
* Inspired from the interface of Qt.
*/
class Mutex {
public:
  Mutex() ;
  ~Mutex();
  void lock() ;
  void unlock();
private:
  friend CondVar;
  pthread_mutex_t m_mutex;
};

} // namespace cta::threading
