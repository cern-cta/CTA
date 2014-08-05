#include "castor/server/Mutex.hpp"

//------------------------------------------------------------------------------
//constructor
//------------------------------------------------------------------------------
castor::server::Mutex::Mutex()  {
  pthread_mutexattr_t attr;
  castor::exception::Errnum::throwOnReturnedErrno(
    pthread_mutexattr_init(&attr),
    "Error from pthread_mutexattr_init in castor::server::Mutex::Mutex()");
  castor::exception::Errnum::throwOnReturnedErrno(
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK),
    "Error from pthread_mutexattr_settype in castor::server::Mutex::Mutex()");
  castor::exception::Errnum::throwOnReturnedErrno(
    pthread_mutex_init(&m_mutex, &attr),
    "Error from pthread_mutex_init in castor::server::Mutex::Mutex()");
  try {
    castor::exception::Errnum::throwOnReturnedErrno(
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
  castor::exception::Errnum::throwOnReturnedErrno(
    pthread_mutex_lock(&m_mutex),
    "Error from pthread_mutex_lock in castor::server::Mutex::lock()");
}
//------------------------------------------------------------------------------
//unlock
//------------------------------------------------------------------------------
void castor::server::Mutex::unlock()  {
  castor::exception::Errnum::throwOnReturnedErrno(
  pthread_mutex_unlock(&m_mutex),
          "Error from pthread_mutex_unlock in castor::server::Mutex::unlock()");
}
