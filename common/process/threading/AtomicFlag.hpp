/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/process/threading/MutexLocker.hpp"
#include "common/process/threading/Thread.hpp"

namespace cta::threading {

// A one-way flag: once set, it cannot be reset
struct AtomicFlag {
  AtomicFlag() = default;

  void set() { m_set = true; }

  operator bool() const { return m_set; }

private:
  std::atomic<bool> m_set = false;
};

}  // namespace cta::threading
