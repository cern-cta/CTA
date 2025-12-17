/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/process/threading/RWLockWrLocker.hpp"

#include "common/exception/Exception.hpp"
#include "common/process/threading/RWLock.hpp"

namespace cta::threading {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RWLockWrLocker::RWLockWrLocker(RWLock& lock) : m_lock(lock) {
  try {
    m_lock.wrlock();
  } catch (exception::Exception& ne) {
    throw exception::Exception(" Failed to take write lock: " + ne.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
RWLockWrLocker::~RWLockWrLocker() {
  m_lock.unlock();
}

}  // namespace cta::threading
