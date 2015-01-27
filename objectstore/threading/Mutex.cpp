#include "threading/Mutex.hpp"
#include "exception/Errnum.hpp"
#include "exception/Exception.hpp"

//------------------------------------------------------------------------------
//constructor
//------------------------------------------------------------------------------
cta::threading::Mutex::Mutex()  {
  pthread_mutexattr_t attr;
  cta::exception::Errnum::throwOnReturnedErrno(
    pthread_mutexattr_init(&attr),
    "Error from pthread_mutexattr_init in cta::threading::Mutex::Mutex()");
  cta::exception::Errnum::throwOnReturnedErrno(
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK),
    "Error from pthread_mutexattr_settype in cta::threading::Mutex::Mutex()");
  cta::exception::Errnum::throwOnReturnedErrno(
    pthread_mutex_init(&m_mutex, &attr),
    "Error from pthread_mutex_init in cta::threading::Mutex::Mutex()");
  try {
    cta::exception::Errnum::throwOnReturnedErrno(
      pthread_mutexattr_destroy(&attr),
      "Error from pthread_mutexattr_destroy in cta::threading::Mutex::Mutex()");
  } catch (...) {
    pthread_mutex_destroy(&m_mutex);
    throw;
  }
}
//------------------------------------------------------------------------------
//destructor
//------------------------------------------------------------------------------
cta::threading::Mutex::~Mutex() {
  pthread_mutex_destroy(&m_mutex);
}
//------------------------------------------------------------------------------
//lock
//------------------------------------------------------------------------------
void cta::threading::Mutex::lock()  {
  cta::exception::Errnum::throwOnReturnedErrno(
    pthread_mutex_lock(&m_mutex),
    "Error from pthread_mutex_lock in cta::threading::Mutex::lock()");
}
//------------------------------------------------------------------------------
//unlock
//------------------------------------------------------------------------------
void cta::threading::Mutex::unlock()  {
  cta::exception::Errnum::throwOnReturnedErrno(
  pthread_mutex_unlock(&m_mutex),
          "Error from pthread_mutex_unlock in cta::threading::Mutex::unlock()");
}
