#include "castor/server/MutexLocker.hpp"
#include "castor/server/Semaphores.hpp"
#include "castor/server/Threading.hpp"
#include "castor/exception/Errnum.hpp"
#include "common/exception/Exception.hpp"
#include <errno.h>
#include <sys/time.h>

//------------------------------------------------------------------------------
//PosixSemaphore constructor
//------------------------------------------------------------------------------
castor::server::PosixSemaphore::PosixSemaphore(int initial)
 {
  castor::exception::Errnum::throwOnReturnedErrno(
    sem_init(&m_sem, 0, initial),
    "Error from sem_init in castor::server::PosixSemaphore::PosixSemaphore()");
}
//------------------------------------------------------------------------------
//PosixSemaphore destructor
//------------------------------------------------------------------------------
castor::server::PosixSemaphore::~PosixSemaphore() {
  /* There is a danger of destroying the semaphore in the consumer
     while the producer is still referring to the object.
     This mutex prevents this from happening. (The release method locks it). */
  MutexLocker ml(&m_mutexPosterProtection);
  sem_destroy(&m_sem);
}
//------------------------------------------------------------------------------
//acquire
//------------------------------------------------------------------------------
void castor::server::PosixSemaphore::acquire()
 {
  int ret;
  /* If we receive EINTR, we should just keep trying (signal interruption) */
  while((ret = sem_wait(&m_sem)) && EINTR == errno) {}
  /* If it was not EINTR, it's a failure */
  castor::exception::Errnum::throwOnNonZero(ret,
    "Error from sem_wait in castor::server::PosixSemaphore::acquire()");
}
//------------------------------------------------------------------------------
//acquire
//------------------------------------------------------------------------------
void castor::server::PosixSemaphore::acquireWithTimeout(uint64_t timeout_us)
 {
  int ret;
  struct timeval tv;
  gettimeofday(&tv, NULL);
  struct timespec ts;
  // Add microseconds
  ts.tv_nsec = (tv.tv_usec + (timeout_us % 1000000)) * 1000;
  // Forward carry and add seconds
  ts.tv_sec = tv.tv_sec + timeout_us / 1000000 + ts.tv_nsec / 1000000000;
  // Clip what we carried
  ts.tv_nsec %= 1000000000;
  /* If we receive EINTR, we should just keep trying (signal interruption) */
  while((ret = sem_timedwait(&m_sem, &ts)) && EINTR == errno) {}
  /* If we got a timeout, throw a special exception */
  if (ret && ETIMEDOUT == errno) { throw Timeout(); }
  /* If it was not EINTR, it's a failure */
  castor::exception::Errnum::throwOnNonZero(ret,
    "Error from sem_wait in castor::server::PosixSemaphore::acquireWithTimeout()");
}

//------------------------------------------------------------------------------
//tryAcquire
//------------------------------------------------------------------------------
bool castor::server::PosixSemaphore::tryAcquire()
 {
  int ret = sem_trywait(&m_sem);
  if (!ret) return true;
  if (ret && EAGAIN == errno) return false;
  castor::exception::Errnum::throwOnNonZero(ret,
    "Error from sem_trywait in castor::server::PosixSemaphore::tryAcquire()");
  /* unreacheable, just for compiler happiness */
  return false;
}
//------------------------------------------------------------------------------
//release
//------------------------------------------------------------------------------
void castor::server::PosixSemaphore::release(int n)
 {
  for (int i=0; i<n; i++) {
    MutexLocker ml(&m_mutexPosterProtection);
    castor::exception::Errnum::throwOnNonZero(sem_post(&m_sem),
      "Error from sem_post in castor::server::PosixSemaphore::release()");
  }
}
//------------------------------------------------------------------------------
//CondVarSemaphore constructor
//------------------------------------------------------------------------------
castor::server::CondVarSemaphore::CondVarSemaphore(int initial)
:m_value(initial) {
      castor::exception::Errnum::throwOnReturnedErrno(
        pthread_cond_init(&m_cond, NULL),
        "Error from pthread_cond_init in castor::server::CondVarSemaphore::CondVarSemaphore()");
      castor::exception::Errnum::throwOnReturnedErrno(
        pthread_mutex_init(&m_mutex, NULL),
        "Error from pthread_mutex_init in castor::server::CondVarSemaphore::CondVarSemaphore()");
    }

//------------------------------------------------------------------------------
//CondVarSemaphore destructor
//------------------------------------------------------------------------------
castor::server::CondVarSemaphore::~CondVarSemaphore() {
      /* Barrier protecting the last user */
      pthread_mutex_lock(&m_mutex);
      pthread_mutex_unlock(&m_mutex);
      /* Cleanup */
      int rc=pthread_cond_destroy(&m_cond);
      rc=rc;
      pthread_mutex_destroy(&m_mutex);
    }
//------------------------------------------------------------------------------
//acquire
//------------------------------------------------------------------------------
void castor::server::CondVarSemaphore::acquire()
 {
  castor::exception::Errnum::throwOnReturnedErrno(
    pthread_mutex_lock(&m_mutex),
    "Error from pthread_mutex_lock in castor::server::CondVarSemaphore::acquire()");
  while (m_value <= 0) {
    castor::exception::Errnum::throwOnReturnedErrno(
      pthread_cond_wait(&m_cond, &m_mutex),
      "Error from pthread_cond_wait in castor::server::CondVarSemaphore::acquire()");
  }
  m_value--;
  castor::exception::Errnum::throwOnReturnedErrno(
    pthread_mutex_unlock(&m_mutex),
    "Error from pthread_mutex_unlock in castor::server::CondVarSemaphore::acquire()");
}
//------------------------------------------------------------------------------
//tryAcquire
//------------------------------------------------------------------------------
bool castor::server::CondVarSemaphore::tryAcquire()
 {
  bool ret;
  castor::exception::Errnum::throwOnReturnedErrno(
    pthread_mutex_lock(&m_mutex),
      "Error from pthread_mutex_lock in castor::server::CondVarSemaphore::tryAcquire()");
  if (m_value > 0) {
    ret = true;
    m_value--;
  } else {
    ret = false;
  }
  castor::exception::Errnum::throwOnReturnedErrno(
    pthread_mutex_unlock(&m_mutex),
      "Error from pthread_mutex_unlock in castor::server::CondVarSemaphore::tryAcquire()");
  return ret;
}
//------------------------------------------------------------------------------
//release
//------------------------------------------------------------------------------
void castor::server::CondVarSemaphore::release(int n)
 {
  for (int i=0; i<n; i++) {
  castor::exception::Errnum::throwOnReturnedErrno(
    pthread_mutex_lock(&m_mutex),
      "Error from pthread_mutex_unlock in castor::server::CondVarSemaphore::release()");
    m_value++;
  castor::exception::Errnum::throwOnReturnedErrno(
    pthread_cond_signal(&m_cond),
      "Error from pthread_cond_signal in castor::server::CondVarSemaphore::release()");
  castor::exception::Errnum::throwOnReturnedErrno(
    pthread_mutex_unlock(&m_mutex),
      "Error from pthread_mutex_unlock in castor::server::CondVarSemaphore::release()");
  }
}
