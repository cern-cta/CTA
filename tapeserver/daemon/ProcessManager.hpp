/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "SubprocessHandler.hpp"
#include "common/log/LogContext.hpp"
#include <memory>
#include <list>

namespace cta::tape::daemon {

/**
 * A class managing several subprocesses, through their handlers. The subprocess
 * handler keeps track of all handlers and owns them.
 */
class ProcessManager {
public:
  explicit ProcessManager(log::LogContext & log);
  virtual ~ProcessManager();
  /** Function passing ownership of a subprocess handler to the manager. */
  void addHandler(std::unique_ptr<SubprocessHandler> && handler);
  /** Function allowing a SubprocessHandler to register a file descriptor to epoll */
  virtual void addFile(int fd, SubprocessHandler * sh);
  /** Function allowing a SubprocessHandler to unregister a file descriptor from epoll */
  virtual void removeFile(int fd);
  /** Infinite loop of the process. This function returns and exit value for
   * the whole process. Implements the loop described in the header file for
   * SubprocessHandler class. */
  int run();
  /** Get reference to a given handler */
  SubprocessHandler & at(const std::string & name);
  /** Get reference to the log context */
  log::LogContext & logContext();
private:
  log::LogContext & m_logContext; ///< The log context
  int m_epollFd;   ///< The file descriptor for the epoll interface
  /// Structure allowing the follow up of subprocesses
  struct SubprocessAndStatus {
    SubprocessHandler::ProcessingStatus status;
    std::unique_ptr<SubprocessHandler> handler;
  };
  /// The list of process handlers we own and their statuses.
  std::list<SubprocessAndStatus> m_subprocessHandlers;
  /// Status report structs for run parts
  struct RunPartStatus {
    bool doExit=false;
    int exitCode=0;
  };
  /// sub part for run(): handle shutdown
  RunPartStatus runShutdownManagement();
  /// subpart for run(): handle kill
  RunPartStatus runKillManagement();
  /// subpart for run(): handle forking
  RunPartStatus runForkManagement();
  /// subpart for run(): handle SIGCHLD
  RunPartStatus runSigChildManagement();
  /// subpart for run(): handle refresh logger requests
  RunPartStatus runRefreshLoggerManagement();
  /// subpart for run(): handle events and timeouts. Does not return any value.
  void runEventLoop();
};

} // namespace cta::tape::daemon
