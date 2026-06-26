/*
 * SPDX-FileCopyrightText: 2021 CERN, 2026 DESY
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/Exception.hpp"

#include <optional>
#include <thread>

namespace cta::threading {

/**
 * An exception class thrown by the Thread class.
 */
CTA_GENERATE_EXCEPTION_CLASS(UncaughtExceptionInThread);

/**
 * A Thread class, based on the Qt interface. To be used, one should
 * inherit from it, and implement the run() method.
 * The thread is started with start() and joined with wait().
 * Now uses std::jthread instead of pthread_t.
 */
class Thread {
public:
  Thread() = default;

  explicit Thread(std::optional<size_t> stackSize) : m_hadException(false), m_what(""), m_stackSize(stackSize) {}

  virtual ~Thread() = default;
  void start();
  void wait() const;
  void kill() const;

protected:
  virtual void run() = 0;

private:
  mutable std::jthread m_thread;
  mutable bool m_hadException = false;
  mutable std::string m_what;
  mutable std::string m_type;
  std::optional<size_t> m_stackSize;
};

}  // namespace cta::threading
