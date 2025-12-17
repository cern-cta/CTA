/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "SubprocessHandler.hpp"

namespace cta::tape::daemon {

class ProcessManager;

//! A virtual process handler managing the signals via signalfd

class SignalHandler : public SubprocessHandler {
public:
  explicit SignalHandler(ProcessManager& pm);
  ~SignalHandler() override;
  ProcessingStatus fork() override;
  ProcessingStatus getInitialStatus() override;
  void kill() override;
  ProcessingStatus processSigChild() override;
  ProcessingStatus processRefreshLoggerRequest() override;
  ProcessingStatus refreshLogger() override;
  void postForkCleanup() override;
  ProcessingStatus processEvent() override;
  ProcessingStatus processTimeout() override;
  int runChild() override;
  ProcessingStatus shutdown() override;

  template<class C>
  void setTimeout(C timeout) {
    m_timeoutDuration = std::chrono::duration_cast<decltype(m_timeoutDuration)>(timeout);
  }

private:
  cta::tape::daemon::ProcessManager& m_processManager;
  int m_sigFd = -1;
  bool m_shutdownRequested = false;
  bool m_shutdownAcknowlegded = false;
  bool m_refreshLoggerRequested = false;
  bool m_sigChildPending = false;
  std::chrono::time_point<std::chrono::steady_clock> m_shutdownStartTime = decltype(m_shutdownStartTime)::max();
  std::chrono::milliseconds m_timeoutDuration =
    std::chrono::duration_cast<decltype(m_timeoutDuration)>(std::chrono::minutes(9));
  // The current state we report to process manager
  ProcessingStatus m_processingStatus;
};

}  // namespace cta::tape::daemon
