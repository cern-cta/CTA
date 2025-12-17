/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/process/threading/RWLockRdLocker.hpp"

#include "common/exception/Exception.hpp"
#include "common/process/threading/RWLock.hpp"

namespace cta::threading {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RWLockRdLocker::RWLockRdLocker(RWLock& lock) : m_lock(lock) {
  try {
    m_lock.rdlock();
  } catch (exception::Exception& ne) {
    throw exception::Exception("Failed to take read lock: " + ne.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
RWLockRdLocker::~RWLockRdLocker() {
  m_lock.unlock();
}

}  // namespace cta::threading
