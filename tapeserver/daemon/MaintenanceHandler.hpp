/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "SubprocessHandler.hpp"
#include "ProcessManager.hpp"
#include "tapeserver/daemon/common/TapedConfiguration.hpp"
#include "common/threading/SocketPair.hpp"

namespace cta::tape::daemon {

/**
 * Handler for garbage collector subprocesses. This long lived process should live
 * as long as the main process, but will be respawned in case of crash.
 */
class MaintenanceHandler: public SubprocessHandler {
public:
  MaintenanceHandler(const common::TapedConfiguration & tapedConfig, ProcessManager & pm);
  virtual ~MaintenanceHandler();
  ProcessingStatus getInitialStatus() override;
  ProcessingStatus fork() override;
  void postForkCleanup() override;
  int runChild() noexcept override;
  ProcessingStatus shutdown() override;
  void kill() override;
  ProcessingStatus processEvent() override;
  ProcessingStatus processSigChild() override;
  ProcessingStatus processRefreshLoggerRequest() override;
  ProcessingStatus refreshLogger() override;
  ProcessingStatus processTimeout() override;
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
  const common::TapedConfiguration & m_tapedConfig;
  /** The current state we report to process manager */
  ProcessingStatus m_processingStatus;
  /** PID for the subprocess */
  pid_t m_pid=-1;
  /** Keep track of the shutdown state */
  bool m_shutdownInProgress=false;
  /** A socketpair to ask the child process to gracefully shut down */
  std::unique_ptr<cta::server::SocketPair> m_socketPair;
  /** The poll period for the garbage collector */
  static const time_t s_pollInterval = 10;

  static constexpr const char* const SHUTDOWN_MSG = "SHUTDOWN";
  static constexpr const char* const REFRESH_LOGGER_MSG = "REFRESH_LOGGER";
};

} // namespace cta::tape::daemon
