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

#include "SignalReactor.hpp"

#include <signal.h>
#include <sys/prctl.h>
#include <chrono>
#include <thread>

#include "common/exception/Errnum.hpp"
#include "common/semconv/Attributes.hpp"
#include "SignalUtils.hpp"

namespace cta::process {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SignalReactor::SignalReactor(cta::log::LogContext& lc,
                             const sigset_t& sigset,
                             const std::unordered_map<int, std::function<void()>>& signalFunctions)
    : m_lc(lc),
      m_sigset(sigset),
      m_signalFunctions(signalFunctions) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
SignalReactor::~SignalReactor() {
  // Gracefully shutdown the reactor
  stop();
}

//------------------------------------------------------------------------------
// SignalReactor::start
//------------------------------------------------------------------------------
void SignalReactor::start() {
  m_stopRequested = false;
  cta::exception::Errnum::throwOnNonZero(::pthread_sigmask(SIG_BLOCK, &m_sigset, nullptr),
                                         "In SignalReactor::start(): pthread_sigmask() failed");
  m_thread = std::jthread(&SignalReactor::run, this);
}

//------------------------------------------------------------------------------
// SignalReactor::stop
//------------------------------------------------------------------------------
void SignalReactor::stop() noexcept {
  m_stopRequested = true;
  if (m_thread.joinable()) {
    try {
      m_thread.join();
    } catch (std::system_error& e) {
      log::ScopedParamContainer params(m_lc);
      params.add("exceptionMessage", e.what());
      m_lc.log(log::ERR, "In SignalReactor::stop(): failed to join thread");
    }
  }
}

//------------------------------------------------------------------------------
// SignalReactor::run
//------------------------------------------------------------------------------
void SignalReactor::run() {
  m_lc.log(log::INFO, "In SignalReactor::run(): Starting SignalReactor");
  timespec ts;
  ts.tv_sec = m_waitTimeoutSecs;
  ts.tv_nsec = 0;

  try {
    while (!m_stopRequested) {
      siginfo_t si {};
      int signal = sigtimedwait(&m_sigset, &si, &ts);
      // Handle errors
      if (signal == -1) {
        int e = errno;
        // Just a timeout
        if (e == EAGAIN || e == EINTR) {
          continue;
        }
        // Something else
        log::ScopedParamContainer params(m_lc);
        params.add("errno", std::to_string(e));
        params.add("errorMessage", ::strerror(e));
        m_lc.log(log::WARNING, "In SignalReactor::run(): sigtimedwait failed");
        continue;
      }
      m_lc.log(log::INFO, "In SignalReactor::run(): received " + utils::signalToString(signal));
      // Check whether we have something to do for this signal
      if (!m_signalFunctions.contains(signal)) {
        log::ScopedParamContainer params(m_lc);
        params.add("signal", utils::signalToString(signal));
        m_lc.log(log::INFO, "In SignalReactor::run(): no action for signal");
        continue;
      }

      m_signalFunctions[signal]();
    }
  } catch (std::exception& ex) {
    log::ScopedParamContainer exParams(m_lc);
    exParams.add("exceptionMessage", ex.what());
    m_lc.log(log::ERR, "In SignalReactor::run(): received a std::exception.");
    throw ex;
  } catch (...) {
    m_lc.log(log::ERR, "In SignalReactor::run(): received an unknown exception.");
    throw;
  }
}

}  // namespace cta::process
