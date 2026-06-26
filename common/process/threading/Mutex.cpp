/*
 * SPDX-FileCopyrightText: 2021 CERN, 2026 DESY
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/process/threading/Mutex.hpp"

#include "common/exception/Exception.hpp"

namespace cta::threading {

//------------------------------------------------------------------------------
// lock
//------------------------------------------------------------------------------
void Mutex::lock() {
  try {
    m_mutex.lock();
  } catch (const std::exception& e) {
    throw exception::Exception(std::string("Error from mutex.lock() in cta::threading::Mutex::lock(): ") + e.what());
  }
}

//------------------------------------------------------------------------------
// unlock
//------------------------------------------------------------------------------
void Mutex::unlock() {
  try {
    m_mutex.unlock();
  } catch (const std::exception& e) {
    throw exception::Exception(std::string("Error from mutex.unlock() in cta::threading::Mutex::unlock(): ")
                               + e.what());
  }
}

}  // namespace cta::threading
