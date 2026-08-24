/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "SignalReactor.hpp"

#include "SignalUtils.hpp"
#include "common/exception/Errnum.hpp"
#include "common/log/LogContext.hpp"
#include "common/semconv/Attributes.hpp"

#include <chrono>
#include <poll.h>
#include <signal.h>
#include <sys/prctl.h>
#include <thread>

namespace cta::runtime {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SignalReactor::SignalReactor(cta::log::Logger& log,
                             const std::unordered_map<int, std::function<void()>>& signalFunctions,
                             uint32_t waitTimeoutMsecs)
    : m_log(log),
      m_signalFunctions(signalFunctions),
      m_waitTimeoutMsecs(waitTimeoutMsecs) {}

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
  if (m_hasStarted) {
    throw exception::Exception("In SignalReactor::start(): SignalReactor cannot be started more than once");
  }
  m_log(log::DEBUG, "In SignalReactor::start(): Blocking and registering signals");
  cta::exception::Errnum::throwOnMinusOne(::sigemptyset(&m_sigset), "In SignalReactor::start(): sigemptyset() failed");
  for (const auto& [signal, func] : m_signalFunctions) {
    if (signal == SIGKILL || signal == SIGSTOP || !func) {
      throw exception::Exception("In SignalReactor::start(): invalid callback registration for signal "
                                 + std::to_string(signal));
    }
    cta::exception::Errnum::throwOnMinusOne(::sigaddset(&m_sigset, signal),
                                            "In SignalReactor::start(): sigaddset() failed");
  }
  if (m_signalFunctions.empty()) {
    // An otherwise unused blocked signal is needed to wake an empty reactor.
    m_wakeupSignal = SIGRTMIN;
    cta::exception::Errnum::throwOnMinusOne(::sigaddset(&m_sigset, m_wakeupSignal),
                                            "In SignalReactor::start(): failed to add the wake-up signal");
  } else {
    // Reuse a blocked signal to interrupt sigtimedwait() during shutdown.
    // run() checks the stop token before dispatching, so this wake-up never invokes the registered callback.
    m_wakeupSignal = m_signalFunctions.begin()->first;
  }
  m_hasStarted = true;
  cta::exception::Errnum::throwOnNonZero(::pthread_sigmask(SIG_BLOCK, &m_sigset, nullptr),
                                         "In SignalReactor::start(): pthread_sigmask() failed");
  m_thread =
    std::jthread([this](std::stop_token st) { run(st, m_signalFunctions, m_sigset, m_log, m_waitTimeoutMsecs); });
}

//------------------------------------------------------------------------------
// SignalReactor::stop
//------------------------------------------------------------------------------
void SignalReactor::stop() noexcept {
  m_log(log::DEBUG, "In SignalReactor::stop(): stopping SignalReactor");
  m_thread.request_stop();
  if (m_thread.joinable()) {
    // Signal the thread to wake up from its timed wait for faster shutdown
    // We just send an arbitrary blocked signal. The thread itself will check if it should stop before invoking the callback
    const int wakeupRc = ::pthread_kill(m_thread.native_handle(), m_wakeupSignal);
    if (wakeupRc != 0 && wakeupRc != ESRCH) {
      m_log(log::ERR,
            "In SignalReactor::stop(): failed to wake the SignalReactor thread",
            {
              {"errno",                    std::to_string(wakeupRc)},
              {semconv::log::errorMessage, ::strerror(wakeupRc)    }
      });
    }
    try {
      m_thread.join();
    } catch (std::system_error& e) {
      m_log(log::ERR,
            "In SignalReactor::stop(): failed to join thread",
            {
              {semconv::log::exceptionMessage, e.what()}
      });
    }
  }
}

//------------------------------------------------------------------------------
// SignalReactor::run
//------------------------------------------------------------------------------
void SignalReactor::run(std::stop_token st,
                        const std::unordered_map<int, std::function<void()>>& signalFunctions,
                        const sigset_t& sigset,
                        cta::log::Logger& log,
                        const uint32_t waitTimeoutMsecs) {
  cta::log::LogContext lc(log);
  lc.log(log::INFO, "In SignalReactor::run(): Starting SignalReactor");
  timespec ts;
  ts.tv_sec = waitTimeoutMsecs / 1000;
  ts.tv_nsec = (waitTimeoutMsecs % 1000) * 1e6;

  try {
    while (!st.stop_requested()) {
      siginfo_t si {};
      int signal = sigtimedwait(&sigset, &si, &ts);
      // Handle errors
      if (signal == -1) {
        int e = errno;
        // Just a timeout
        if (e == EAGAIN || e == EINTR) {
          continue;
        }
        // Something else
        log::ScopedParamContainer params(lc);
        params.add("errno", std::to_string(e));
        params.add(semconv::log::errorMessage, ::strerror(e));
        lc.log(log::WARNING, "In SignalReactor::run(): sigtimedwait failed");
        continue;
      }
      // Ensure the m_wakeupSignal doesn't invoke an application callback
      if (st.stop_requested()) {
        break;
      }
      lc.log(log::INFO, "In SignalReactor::run(): received " + utils::signalToString(signal));
      try {
        signalFunctions.at(signal)();
      } catch (const std::exception& ex) {
        log::ScopedParamContainer exParams(lc);
        exParams.add("signal", utils::signalToString(signal));
        exParams.add(semconv::log::exceptionMessage, ex.what());
        lc.log(log::ERR, "In SignalReactor::run(): signal callback threw an exception");
      } catch (...) {
        log::ScopedParamContainer exParams(lc);
        exParams.add("signal", utils::signalToString(signal));
        lc.log(log::ERR, "In SignalReactor::run(): signal callback threw an unknown exception");
      }
    }
  } catch (std::exception& ex) {
    log::ScopedParamContainer exParams(lc);
    exParams.add(semconv::log::exceptionMessage, ex.what());
    lc.log(log::ERR, "In SignalReactor::run(): received a std::exception.");
  } catch (...) {
    lc.log(log::ERR, "In SignalReactor::run(): received an unknown exception.");
  }
  lc.log(log::INFO, "In SignalReactor::run(): SignalReactor stopped listening");
}

}  // namespace cta::runtime
