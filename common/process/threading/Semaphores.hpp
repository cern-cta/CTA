/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/process/threading/Mutex.hpp"

#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>

namespace cta::threading {

/**
 * An exception throwing wrapper to posix semaphores.
 */
class PosixSemaphore {
public:
  class Timeout {};

  explicit PosixSemaphore(int initial = 0);
  ~PosixSemaphore();
  void acquire();
  void acquireWithTimeout(uint64_t timeout_us); /**< Throws an exception (Timeout) in case of timeout */
  bool tryAcquire();
  void release(int n = 1);

private:
  sem_t m_sem;
  /* this mutex protects against destruction unser the feet of the last
   * the poster (race condition in glibc) */
  Mutex m_mutexPosterProtection;
};

class Semaphore : public PosixSemaphore {
public:
  explicit Semaphore(int initial = 0) : PosixSemaphore(initial) {}
};

}  // namespace cta::threading
