/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-FileCopyrightText: 2026 DESY
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/process/threading/Semaphores.hpp"

#include <limits.h>
#include <stdexcept>

namespace cta::threading {

//------------------------------------------------------------------------------
// CountingSemaphore constructor
//------------------------------------------------------------------------------
CountingSemaphore::CountingSemaphore(int initial) : m_sem(initial) {
  if (initial < 0) {
    throw std::invalid_argument("Semaphore initial value cannot be negative");
  }
}

//------------------------------------------------------------------------------
// acquire - blocking acquire
//------------------------------------------------------------------------------
void CountingSemaphore::acquire() {
  m_sem.acquire();
}

//------------------------------------------------------------------------------
// acquireWithTimeout - blocking acquire with timeout
//------------------------------------------------------------------------------
void CountingSemaphore::acquireWithTimeout(uint64_t timeout_us) {
  auto deadline = std::chrono::high_resolution_clock::now() + std::chrono::microseconds(timeout_us);

  if (!m_sem.try_acquire_until(deadline)) {
    throw Timeout();
  }
}

//------------------------------------------------------------------------------
// tryAcquire - non-blocking acquire
//------------------------------------------------------------------------------
bool CountingSemaphore::tryAcquire() {
  return m_sem.try_acquire();
}

//------------------------------------------------------------------------------
// release - release the semaphore
//------------------------------------------------------------------------------
void CountingSemaphore::release(int n) {
  for (int i = 0; i < n; ++i) {
    m_sem.release();
  }
}

}  // namespace cta::threading
