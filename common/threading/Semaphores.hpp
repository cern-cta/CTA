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
