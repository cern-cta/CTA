/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2025 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "common/log/LogContext.hpp"

#include <functional>
#include <poll.h>
#include <thread>
#include <unordered_map>

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
                const std::unordered_map<int, std::function<void()>>& signalFunctions);

  ~SignalReactor();

  /**
   * Starts the SignalReactor on a separate thread. Note that this will block all registered signals on all threads when this is called.
   * As such, for this to work correctly, the SignalReactor must be started before any other threads start
   */
  void start();

  /**
   * Waits for incoming signals and executes the functions registered with said signal (if any)
   */
  void run();

  /**
   * Stop the SignalReactor (both the thread and the waiting for signal)
   */
  void stop() noexcept;

private:
  cta::log::LogContext& m_lc;
  sigset_t m_sigset;
  std::unordered_map<int, std::function<void()>> m_signalFunctions;

  // The thread the signalReactor will run on when start() is called
  std::jthread m_thread;
  std::atomic<bool> m_stopRequested;

  uint32_t m_waitTimeoutSecs = 1;
};

}  // namespace cta::process
