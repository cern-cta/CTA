/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "SubprocessHandler.hpp"
#include "ProcessManager.hpp"
#include "TapedConfiguration.hpp"
#include "common/threading/SocketPair.hpp"

namespace cta { namespace tape { namespace  daemon {

/**
 * Handler for garbage collector subprocesses. This long lived process should live
 * as long as the main process, but will be respawned in case of crash.
 */
class MaintenanceHandler: public SubprocessHandler {
public:
  MaintenanceHandler(const TapedConfiguration & tapedConfig, ProcessManager & pm);
  virtual ~MaintenanceHandler();
  SubprocessHandler::ProcessingStatus getInitialStatus() override;
  SubprocessHandler::ProcessingStatus fork() override;
  void postForkCleanup() override;
  int runChild() override;
  SubprocessHandler::ProcessingStatus shutdown() override;
  void kill() override;
  SubprocessHandler::ProcessingStatus processEvent() override;
  SubprocessHandler::ProcessingStatus processSigChild() override;
  SubprocessHandler::ProcessingStatus processTimeout() override;
private:
  void exceptionThrowingRunChild();
  
  /**
   * Returns true if the RepackRequestManager will be ran on this tapeserver,
   * false otherwise
   */
  bool runRepackRequestManager() const;
  /** Reference to the process manager*/
  cta::tape::daemon::ProcessManager & m_processManager;
  /** The parameters */
  const TapedConfiguration & m_tapedConfig;
  /** The current state we report to process manager */
  SubprocessHandler::ProcessingStatus m_processingStatus;
  /** PID for the subprocess */
  pid_t m_pid=-1;
  /** Keep track of the shutdown state */
  bool m_shutdownInProgress=false;
  /** A socketpair to ask the child process to gracefully shut down */
  std::unique_ptr<cta::server::SocketPair> m_socketPair;
  /** The poll period for the garbage collector */
  static const time_t s_pollInterval = 10;
};

}}} // namespace cta::tape::daemon
