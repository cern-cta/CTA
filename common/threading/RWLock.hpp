/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <pthread.h>

namespace cta::threading {

/**
 * A C++ wrapper around a pthdead read-write lock.
 * variable.
 */
class RWLock {
public:
  RWLock();
  ~RWLock();
  void rdlock();
  void wrlock();
  void unlock();

private:
  pthread_rwlock_t m_lock;
};

} // namespace cta::threading
