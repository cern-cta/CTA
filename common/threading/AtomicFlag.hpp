/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */


#pragma once

#include "common/threading/MutexLocker.hpp"
#include "common/threading/Thread.hpp"

namespace cta::threading {

// A one-way flag: once set, it cannot be reset
struct AtomicFlag {
  AtomicFlag() : m_set(false) {};
  void set() {
    MutexLocker ml(m_mutex);
    m_set = true;
  }
  operator bool() const {
    MutexLocker ml(m_mutex);
    return m_set;
  }
private:
  bool m_set;
  mutable Mutex m_mutex;
};

} // namespace cta::threading
