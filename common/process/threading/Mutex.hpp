/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-FileCopyrightText: 2026 DESY
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <mutex>

namespace cta::threading {

/**
 * Forward declaration of the friend class that represents a condition variable.
 */
class CondVar;

/**
 * A simple exception throwing wrapper for std::mutex
 */
class Mutex {
public:
  Mutex() = default;
  ~Mutex() = default;
  void lock();
  void unlock();

private:
  friend CondVar;
  std::mutex m_mutex;
};

}  // namespace cta::threading
