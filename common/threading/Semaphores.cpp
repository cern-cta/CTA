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

#include "common/threading/MutexLocker.hpp"
#include "common/threading/Semaphores.hpp"
#include "common/threading/Thread.hpp"
#include "common/exception/Errnum.hpp"
#include "common/exception/Exception.hpp"
#include <errno.h>
#include <sys/time.h>

namespace cta {
namespace threading {

//------------------------------------------------------------------------------
//PosixSemaphore constructor
//------------------------------------------------------------------------------
PosixSemaphore::PosixSemaphore(int initial) {
  cta::exception::Errnum::throwOnReturnedErrno(
    sem_init(&m_sem, 0, initial), "Error from sem_init in cta::threading::PosixSemaphore::PosixSemaphore()");
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

//------------------------------------------------------------------------------
//CondVarSemaphore constructor
//------------------------------------------------------------------------------
CondVarSemaphore::CondVarSemaphore(int initial) : m_value(initial) {
  cta::exception::Errnum::throwOnReturnedErrno(
    pthread_cond_init(&m_cond, nullptr),
    "Error from pthread_cond_init in cta::threading::CondVarSemaphore::CondVarSemaphore()");
  cta::exception::Errnum::throwOnReturnedErrno(
    pthread_mutex_init(&m_mutex, nullptr),
    "Error from pthread_mutex_init in cta::threading::CondVarSemaphore::CondVarSemaphore()");
}

//------------------------------------------------------------------------------
//CondVarSemaphore destructor
//------------------------------------------------------------------------------
CondVarSemaphore::~CondVarSemaphore() {
  /* Barrier protecting the last user */
  pthread_mutex_lock(&m_mutex);
  pthread_mutex_unlock(&m_mutex);
  /* Cleanup */
  pthread_cond_destroy(&m_cond);
  pthread_mutex_destroy(&m_mutex);
}

//------------------------------------------------------------------------------
//acquire
//------------------------------------------------------------------------------
void CondVarSemaphore::acquire() {
  cta::exception::Errnum::throwOnReturnedErrno(
    pthread_mutex_lock(&m_mutex), "Error from pthread_mutex_lock in cta::threading::CondVarSemaphore::acquire()");
  while (m_value <= 0) {
    cta::exception::Errnum::throwOnReturnedErrno(
      pthread_cond_wait(&m_cond, &m_mutex),
      "Error from pthread_cond_wait in cta::threading::CondVarSemaphore::acquire()");
  }
  m_value--;
  cta::exception::Errnum::throwOnReturnedErrno(
    pthread_mutex_unlock(&m_mutex), "Error from pthread_mutex_unlock in cta::threading::CondVarSemaphore::acquire()");
}

//------------------------------------------------------------------------------
//tryAcquire
//------------------------------------------------------------------------------
bool CondVarSemaphore::tryAcquire() {
  bool ret;
  cta::exception::Errnum::throwOnReturnedErrno(
    pthread_mutex_lock(&m_mutex), "Error from pthread_mutex_lock in cta::threading::CondVarSemaphore::tryAcquire()");
  if (m_value > 0) {
    ret = true;
    m_value--;
  }
  else {
    ret = false;
  }
  cta::exception::Errnum::throwOnReturnedErrno(
    pthread_mutex_unlock(&m_mutex),
    "Error from pthread_mutex_unlock in cta::threading::CondVarSemaphore::tryAcquire()");
  return ret;
}

//------------------------------------------------------------------------------
//release
//------------------------------------------------------------------------------
void CondVarSemaphore::release(int n) {
  for (int i = 0; i < n; i++) {
    cta::exception::Errnum::throwOnReturnedErrno(
      pthread_mutex_lock(&m_mutex), "Error from pthread_mutex_unlock in cta::threading::CondVarSemaphore::release()");
    m_value++;
    cta::exception::Errnum::throwOnReturnedErrno(
      pthread_cond_signal(&m_cond), "Error from pthread_cond_signal in cta::threading::CondVarSemaphore::release()");
    cta::exception::Errnum::throwOnReturnedErrno(
      pthread_mutex_unlock(&m_mutex), "Error from pthread_mutex_unlock in cta::threading::CondVarSemaphore::release()");
  }
}

}  // namespace threading
}  // namespace cta
