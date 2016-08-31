#include "castor/messages/Mutex.hpp"
#include "common/exception/Errnum.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
//constructor
//------------------------------------------------------------------------------
castor::messages::Mutex::Mutex()  {
  pthread_mutexattr_t attr;
  cta::exception::Errnum::throwOnReturnedErrno(
    pthread_mutexattr_init(&attr),
    "Error from pthread_mutexattr_init in castor::messages::Mutex::Mutex()");
  cta::exception::Errnum::throwOnReturnedErrno(
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK),
    "Error from pthread_mutexattr_settype in castor::messages::Mutex::Mutex()");
  cta::exception::Errnum::throwOnReturnedErrno(
    pthread_mutex_init(&m_mutex, &attr),
    "Error from pthread_mutex_init in castor::messages::Mutex::Mutex()");
  try {
    cta::exception::Errnum::throwOnReturnedErrno(
      pthread_mutexattr_destroy(&attr),
      "Error from pthread_mutexattr_destroy in"
      " castor::messages::Mutex::Mutex()");
  } catch (...) {
    pthread_mutex_destroy(&m_mutex);
    throw;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::messages::Mutex::~Mutex() throw() {
  pthread_mutex_destroy(&m_mutex);
}

//------------------------------------------------------------------------------
// lock
//------------------------------------------------------------------------------
void castor::messages::Mutex::lock()  {
  cta::exception::Errnum::throwOnReturnedErrno(
    pthread_mutex_lock(&m_mutex),
    "Error from pthread_mutex_lock in castor::messages::Mutex::lock()");
}

//------------------------------------------------------------------------------
// unlock
//------------------------------------------------------------------------------
void castor::messages::Mutex::unlock()  {
  cta::exception::Errnum::throwOnReturnedErrno(
  pthread_mutex_unlock(&m_mutex),
    "Error from pthread_mutex_unlock in castor::messages::Mutex::unlock()");
}
