/*
 * SPDX-FileCopyrightText: 2021 CERN, 2026 DESY
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/process/threading/RWLock.hpp"

#include "common/exception/Exception.hpp"

namespace cta::threading {

//------------------------------------------------------------------------------
// rdlock
//------------------------------------------------------------------------------
void RWLock::rdlock() {
  try {
    m_lock.lock_shared();
  } catch (const std::exception& e) {
    throw exception::Exception(std::string("Failed to take read lock on shared_mutex: ") + e.what());
  }
}

//------------------------------------------------------------------------------
// wrlock
//------------------------------------------------------------------------------
void RWLock::wrlock() {
  try {
    m_lock.lock();
  } catch (const std::exception& e) {
    throw exception::Exception(std::string("Failed to take write lock on shared_mutex: ") + e.what());
  }
}

//------------------------------------------------------------------------------
// unlock
//------------------------------------------------------------------------------
void RWLock::unlock() {
  // Note: std::shared_mutex::unlock() works for both read and write locks
  try {
    m_lock.unlock();
  } catch (const std::exception& e) {
    throw exception::Exception(std::string("Failed to unlock shared_mutex: ") + e.what());
  }
}

}  // namespace cta::threading
