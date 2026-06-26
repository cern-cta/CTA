/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-FileCopyrightText: 2026 DESY
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
  virtual ~Thread() = default;

  void start();
  void wait() const;

  /**
   * Requests cooperative stop of the running thread.
   *
   * This does not forcibly terminate the thread. Instead, it signals the
   * associated stop state, and the thread is expected to observe the request
   * and exit on its own in a timely manner.
   *
   * The thread implementation should regularly check the stop token, for example:
   *
   * void run() override {
   *   while (!stop_requested()) {
   *     do_work();
   *   }
   * }
   *
   * After calling stop(), the caller should usually call wait() to
   * join the thread and ensure it has finished cleanly.
   *
   * Note: Currently, we don't have any caller of this interface.
   */
  void stop() const;

protected:
  virtual void run() = 0;
  bool stop_requested() const;

private:
  mutable std::jthread m_thread;
  mutable bool m_hadException = false;
  mutable std::string m_what;
  mutable std::string m_type;
  mutable std::stop_token m_stopToken;
};

}  // namespace cta::threading
