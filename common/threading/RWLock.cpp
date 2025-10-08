/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/threading/RWLock.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"

namespace cta::threading {

//------------------------------------------------------------------------------
//constructor
//------------------------------------------------------------------------------
RWLock::RWLock()  {
  const int rc = pthread_rwlock_init(&m_lock, nullptr);
  if(0 != rc) {
    exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << " failed: Failed to initialise underlying pthread read-write lock: " <<
      utils::errnoToString(rc);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
RWLock::~RWLock() {
  pthread_rwlock_destroy(&m_lock);
}

//------------------------------------------------------------------------------
// rdlock
//------------------------------------------------------------------------------
void RWLock::rdlock()  {
  const int rc = pthread_rwlock_rdlock(&m_lock);
  if(0 != rc) {
    exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << " failed: Failed to take read lock on underlying pthread read-write lock: "
      << utils::errnoToString(rc);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// wrlock
//------------------------------------------------------------------------------
void RWLock::wrlock()  {
  const int rc = pthread_rwlock_wrlock(&m_lock);
  if(0 != rc) {
    exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << " failed: Failed to take write lock on underlying pthread read-write lock: "
      << utils::errnoToString(rc);
    throw ex;
  }
}

//------------------------------------------------------------------------------
//unlock
//------------------------------------------------------------------------------
void RWLock::unlock()  {
  const int rc = pthread_rwlock_unlock(&m_lock);
  if(0 != rc) {
    exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << " failed: Failed to unlock underlying pthread read-write lock: " <<
      utils::errnoToString(rc);
    throw ex;
  }
}

} // namespace cta::threading
