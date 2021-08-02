/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "SubprocessHandler.hpp"

namespace cta {
namespace tape {
namespace daemon {

class ProcessManager;

/// A virtual process handler managing the signals via signalfd.

class SignalHandler: public SubprocessHandler {
public:
  SignalHandler(ProcessManager & pm);
  ~SignalHandler() override;
  SubprocessHandler::ProcessingStatus fork() override;
  SubprocessHandler::ProcessingStatus getInitialStatus() override;
  void kill() override;
  SubprocessHandler::ProcessingStatus processSigChild() override;
  void postForkCleanup() override;
  SubprocessHandler::ProcessingStatus processEvent() override;
  SubprocessHandler::ProcessingStatus processTimeout() override;
  int runChild() override;
  SubprocessHandler::ProcessingStatus shutdown() override;
  template <class C>
  void setTimeout(C timeout) {
    m_timeoutDuration=std::chrono::duration_cast<decltype(m_timeoutDuration)>(timeout);
  }
private:
  cta::tape::daemon::ProcessManager & m_processManager;
  int m_sigFd=-1;
  bool m_shutdownRequested=false;
  bool m_shutdownAcknowlegded=false;
  bool m_sigChildPending=false;
  std::chrono::time_point<std::chrono::steady_clock> m_shutdownStartTime=
          decltype(m_shutdownStartTime)::max();
  std::chrono::milliseconds m_timeoutDuration=
          std::chrono::duration_cast<decltype(m_timeoutDuration)>(std::chrono::minutes(9));
};
}}} // namespace cta::tape::daemon