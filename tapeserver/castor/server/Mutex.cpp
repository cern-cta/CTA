#include "castor/server/Mutex.hpp"
#include "common/exception/Errnum.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
//constructor
//------------------------------------------------------------------------------
castor::server::Mutex::Mutex()  {
  pthread_mutexattr_t attr;
  cta::exception::Errnum::throwOnReturnedErrno(
    pthread_mutexattr_init(&attr),
    "Error from pthread_mutexattr_init in castor::server::Mutex::Mutex()");
  cta::exception::Errnum::throwOnReturnedErrno(
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK),
    "Error from pthread_mutexattr_settype in castor::server::Mutex::Mutex()");
  cta::exception::Errnum::throwOnReturnedErrno(
    pthread_mutex_init(&m_mutex, &attr),
    "Error from pthread_mutex_init in castor::server::Mutex::Mutex()");
  try {
    cta::exception::Errnum::throwOnReturnedErrno(
      pthread_mutexattr_destroy(&attr),
      "Error from pthread_mutexattr_destroy in castor::server::Mutex::Mutex()");
  } catch (...) {
    pthread_mutex_destroy(&m_mutex);
    throw;
  }
}
//------------------------------------------------------------------------------
//destructor
//------------------------------------------------------------------------------
castor::server::Mutex::~Mutex() {
  pthread_mutex_destroy(&m_mutex);
}
//------------------------------------------------------------------------------
//lock
//------------------------------------------------------------------------------
void castor::server::Mutex::lock()  {
  cta::exception::Errnum::throwOnReturnedErrno(
    pthread_mutex_lock(&m_mutex),
    "Error from pthread_mutex_lock in castor::server::Mutex::lock()");
}
//------------------------------------------------------------------------------
//unlock
//------------------------------------------------------------------------------
void castor::server::Mutex::unlock()  {
  cta::exception::Errnum::throwOnReturnedErrno(
  pthread_mutex_unlock(&m_mutex),
          "Error from pthread_mutex_unlock in castor::server::Mutex::unlock()");
}
