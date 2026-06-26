/*
 * SPDX-FileCopyrightText: 2021 CERN, 2026 DESY
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/process/threading/CondVar.hpp"

#include "common/exception/Exception.hpp"
#include "common/process/threading/MutexLocker.hpp"

namespace cta::threading {

//------------------------------------------------------------------------------
// wait
//------------------------------------------------------------------------------
void CondVar::wait(MutexLocker& locker) {
  if (!locker.m_locked) {
    throw exception::Exception("Underlying mutex is not locked.");
  }

  try {
    std::unique_lock<std::mutex> lock(locker.m_mutex.m_mutex, std::adopt_lock);
    m_cond.wait(lock);
    // Release the lock without destroying it (the MutexLocker still owns it)
    lock.release();
  } catch (const std::exception& e) {
    throw exception::Exception(std::string("std::condition_variable::wait failed: ") + e.what());
  }
}

//------------------------------------------------------------------------------
// signal
//------------------------------------------------------------------------------
void CondVar::signal() {
  try {
    m_cond.notify_one();
  } catch (const std::exception& e) {
    throw exception::Exception(std::string("std::condition_variable::notify_one failed: ") + e.what());
  }
}

//------------------------------------------------------------------------------
// broadcast
//------------------------------------------------------------------------------
void CondVar::broadcast() {
  try {
    m_cond.notify_all();
  } catch (const std::exception& e) {
    throw exception::Exception(std::string("std::condition_variable::notify_all failed: ") + e.what());
  }
}

}  // namespace cta::threading
