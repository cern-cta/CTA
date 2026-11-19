/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/process/threading/Semaphores.hpp"

#include "common/exception/Errnum.hpp"
#include "common/exception/Exception.hpp"
#include "common/process/threading/MutexLocker.hpp"
#include "common/process/threading/Thread.hpp"

#include <errno.h>
#include <sys/time.h>

namespace cta::threading {

//------------------------------------------------------------------------------
//PosixSemaphore constructor
//------------------------------------------------------------------------------
PosixSemaphore::PosixSemaphore(int initial) {
  cta::exception::Errnum::throwOnReturnedErrno(
    sem_init(&m_sem, 0, initial),
    "Error from sem_init in cta::threading::PosixSemaphore::PosixSemaphore()");
}

//------------------------------------------------------------------------------
//PosixSemaphore destructor
//------------------------------------------------------------------------------
PosixSemaphore::~PosixSemaphore() {
  /* There is a danger of destroying the semaphore in the consumer
     while the producer is still referring to the object.
     This mutex prevents this from happening. (The release method locks it). */
  MutexLocker ml(m_mutexPosterProtection);
  sem_destroy(&m_sem);
}

//------------------------------------------------------------------------------
//acquire
//------------------------------------------------------------------------------
void PosixSemaphore::acquire() {
  int ret;
  /* If we receive EINTR, we should just keep trying (signal interruption) */
  while ((ret = sem_wait(&m_sem)) && EINTR == errno) {}
  /* If it was not EINTR, it's a failure */
  cta::exception::Errnum::throwOnNonZero(ret, "Error from sem_wait in cta::threading::PosixSemaphore::acquire()");
}

//------------------------------------------------------------------------------
//acquire
//------------------------------------------------------------------------------
void PosixSemaphore::acquireWithTimeout(uint64_t timeout_us) {
  int ret;
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  struct timespec ts;
  // Add microseconds
  ts.tv_nsec = (tv.tv_usec + (timeout_us % 1000000)) * 1000;
  // Forward carry and add seconds
  ts.tv_sec = tv.tv_sec + timeout_us / 1000000 + ts.tv_nsec / 1000000000;
  // Clip what we carried
  ts.tv_nsec %= 1000000000;
  /* If we receive EINTR, we should just keep trying (signal interruption) */
  while ((ret = sem_timedwait(&m_sem, &ts)) && EINTR == errno) {}
  /* If we got a timeout, throw a special exception */
  if (ret && ETIMEDOUT == errno) {
    throw Timeout();
  }
  /* If it was not EINTR, it's a failure */
  cta::exception::Errnum::throwOnNonZero(ret,
                                         "Error from sem_wait in cta::threading::PosixSemaphore::acquireWithTimeout()");
}

//------------------------------------------------------------------------------
//tryAcquire
//------------------------------------------------------------------------------
bool PosixSemaphore::tryAcquire() {
  int ret = sem_trywait(&m_sem);
  if (!ret) {
    return true;
  }
  if (ret && EAGAIN == errno) {
    return false;
  }
  cta::exception::Errnum::throwOnNonZero(ret, "Error from sem_trywait in cta::threading::PosixSemaphore::tryAcquire()");
  /* unreacheable, just for compiler happiness */
  return false;
}

//------------------------------------------------------------------------------
//release
//------------------------------------------------------------------------------
void PosixSemaphore::release(int n) {
  for (int i = 0; i < n; i++) {
    MutexLocker ml(m_mutexPosterProtection);
    cta::exception::Errnum::throwOnNonZero(sem_post(&m_sem),
                                           "Error from sem_post in cta::threading::PosixSemaphore::release()");
  }
}

}  // namespace cta::threading
