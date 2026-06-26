/*
 * SPDX-FileCopyrightText: 2021 CERN, 2026 DESY
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <shared_mutex>

namespace cta::threading {

/**
 * A C++ wrapper around a read-write lock.
 * Now uses std::shared_mutex instead of pthread_rwlock_t.
 */
class RWLock {
public:
  RWLock() = default;
  ~RWLock() = default;
  void rdlock();
  void wrlock();
  void unlock();

private:
  mutable std::shared_mutex m_lock;
};

}  // namespace cta::threading
