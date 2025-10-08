/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>
#include "common/threading/Mutex.hpp"

namespace cta::threading {

/**
   * An exception throwing wrapper to posix semaphores.
   */
  class PosixSemaphore {
  public:
    class Timeout{};
    explicit PosixSemaphore(int initial = 0);
    ~PosixSemaphore();
    void acquire() ;
    void acquireWithTimeout(uint64_t timeout_us); /**< Throws an exception (Timeout) in case of timeout */
    bool tryAcquire() ;
    void release(int n=1) ;
  private:
    sem_t m_sem;
    /* this mutex protects against destruction unser the feet of the last
     *  the poster (race condition in glibc) */
    Mutex m_mutexPosterProtection;
  };

  /**
   * An exception throwing alternative implementation of semaphores, for systems
   * where posix semaphores are not availble (MacOSX at the time of writing)
   */
  class CondVarSemaphore {
  public:
    explicit CondVarSemaphore(int initial = 0);
    ~CondVarSemaphore();
    void acquire() ;
    bool tryAcquire() ;
    void release(int n=1) ;
  private:
    pthread_cond_t m_cond;
    pthread_mutex_t m_mutex;
    int m_value;
  };

  class Semaphore: public PosixSemaphore {
  public:
    explicit Semaphore(int initial = 0) : PosixSemaphore(initial) {}
  };

} // namespace cta::threading
