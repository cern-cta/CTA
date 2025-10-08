/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/Exception.hpp"
#include "common/threading/RWLock.hpp"
#include "common/threading/RWLockWrLocker.hpp"

namespace cta::threading {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RWLockWrLocker::RWLockWrLocker(RWLock &lock): m_lock(lock) {
  try {
    m_lock.wrlock();
  } catch(exception::Exception &ne) {
    exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << " failed to take write lock: " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
RWLockWrLocker::~RWLockWrLocker() {
  m_lock.unlock();
}

} // namespace cta::threading
