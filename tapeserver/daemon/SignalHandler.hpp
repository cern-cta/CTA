/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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
  template <class C>
  void setTimeout(C timeout) {
    m_timeoutDuration=std::chrono::duration_cast<decltype(m_timeoutDuration)>(timeout);
  }
private:
  cta::tape::daemon::ProcessManager & m_processManager;
  int m_sigFd=-1;
  bool m_shutdownRequested=false;
  bool m_shutdownAcknowlegded=false;
  bool m_refreshLoggerRequested=false;
  bool m_sigChildPending=false;
  std::chrono::time_point<std::chrono::steady_clock> m_shutdownStartTime=
          decltype(m_shutdownStartTime)::max();
  std::chrono::milliseconds m_timeoutDuration=
          std::chrono::duration_cast<decltype(m_timeoutDuration)>(std::chrono::minutes(9));
  // The current state we report to process manager
  ProcessingStatus m_processingStatus;
};

} // namespace cta::tape::daemon
