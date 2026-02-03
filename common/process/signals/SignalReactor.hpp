/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"

#include <functional>
#include <poll.h>
#include <thread>
#include <unordered_map>

namespace unitTests {
// See SignalReactorTest.cpp
struct SignalReactorTestAccess;
}  // namespace unitTests

namespace cta::process {

/**
 * Responsible for responding to a certain set of signals.
 * It is important for the SignalReactor to start() before any other threads start,
 * as it needs to correctly block the signals on all threads.
 * It cannot do this if threads are already running.
 */
class SignalReactor {
public:
  SignalReactor(cta::log::LogContext& lc,
                const sigset_t& sigset,
                const std::unordered_map<int, std::function<void()>>& signalFunctions,
                uint32_t waitTimeoutMsecs);

  ~SignalReactor();

  /**
   * Starts the SignalReactor on a separate thread. Note that this will block all registered signals on all threads when this is called.
   * As such, for this to work correctly, the SignalReactor must be started before any other threads start
   */
  void start();

  /**
   * Waits for incoming signals and executes the functions registered with said signal (if any)
   */
  static void run(std::stop_token st,
                  const std::unordered_map<int, std::function<void()>>& signalFunctions,
                  const sigset_t& sigset,
                  cta::log::Logger& log,
                  const uint32_t waitTimeoutMsecs);

  /**
   * Stop the SignalReactor (both the thread and the waiting for signal)
   */
  void stop() noexcept;

private:
  cta::log::LogContext& m_lc;
  const sigset_t m_sigset;
  std::unordered_map<int, std::function<void()>> m_signalFunctions;

  // The thread the signalReactor will run on when start() is called
  std::jthread m_thread;

  const uint32_t m_waitTimeoutMsecs;

  friend struct unitTests::SignalReactorTestAccess;
};

}  // namespace cta::process
