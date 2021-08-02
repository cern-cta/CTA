/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>
#include "common/threading/Mutex.hpp"

namespace cta {
namespace threading {   

/**
   * An exception throwing wrapper to posix semaphores.
   */
  class PosixSemaphore {
  public:
    class Timeout{};
    PosixSemaphore(int initial = 0) ;
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
    CondVarSemaphore(int initial = 0) ;
    ~CondVarSemaphore();
    void acquire() ;
    bool tryAcquire() ;
    void release(int n=1) ;
  private:
    pthread_cond_t m_cond;
    pthread_mutex_t m_mutex;
    int m_value;
  };

#ifndef __APPLE__
  class Semaphore: public PosixSemaphore {
  public:
    Semaphore(int initial=0): PosixSemaphore(initial) {}
  };
#else
  /* Apple does not like posix semaphores :((
     We have to roll our own. */
  class Semaphore: public CondVarSemaphore {
  public:
    Semaphore(int initial=0): CondVarSemaphore(initial) {}
  };
#endif // ndef __APPLE__
  
} // namespace threading
} // namespace cta