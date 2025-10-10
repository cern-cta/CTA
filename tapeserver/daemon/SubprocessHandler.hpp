/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <chrono>
#include <string>
#include <optional>

namespace cta::tape::daemon {

/**
 * The interface to classes managing subprocesses. It allows an external loop
 * to handle global polling and timeouts for a set of them. Several children
 * classes are expected to be developed: a DriveHandler, a MaintenanceHandler
 * and a SignalHandler (using signalfd()).
 * The main loop will typically be:
 * statuses[] = [all]->getInitialStatus(); // This implicitly registers the right fds in epoll (if needed)
 * loop forever
 *   if statuses[any].shutdownRequested
 *     [all]->shutdown()
 *   if statuses[all].completedShutdown()
 *     exit;
 *   if statuses[any].killRequested
 *     [all]->kill()
 *     exit;
 *   if statuses[any].forkRequested()
 *     [all]->prepareToFork()
 *     if [requester]->fork() == child
 *        exit([requester]->runChild())
 *     else
 *        statuses[] = [all]->postFork()
 *   Compute the next timeout statuses[all].timeToTimeout
 *   epoll()
 *
 * This loop allows cooperative preparation for forking, and cleanup after the
 * fork. This could be as simple as a pre-fork noop and a closing of child
 * communication sockets post fork (child side).
 * TODO: post-fork is not yet implemented.
 */
class SubprocessHandler {
public:
  /** The subprocess handler constructor. It will remember its index, as a helper to the manager */
  explicit SubprocessHandler(const std::string& index) : index(index) {}
  const std::string index;          ///< The index of the subprocess (as a helper to the manager)
  virtual ~SubprocessHandler() = default;
  /** An enum allowing the description of the environment (child or parent) */
  enum class ForkState { parent, child, notForking };
  /** The return type for the functions (getStatus, handleEvent, handleTimeout)
   * that will return a status */
  struct ProcessingStatus {
    bool shutdownRequested = false;      ///< Does the process handler require to shutdown all processes?
    bool shutdownComplete = false;       ///< Did this process complete its shutdown?
    bool killRequested = false;          ///< Does the process handler require killing all processes
    bool forkRequested = false;          ///< Does the process handler request to fork a new process?
    bool refreshLoggerRequested = false; ///< Does the process handler request the logger to be refreshed by all subprocesses?
    bool sigChild = false;               ///< Did the process see a SIGCHLD? Used by signal handler only.
    /// Instant of the next timeout for the process handler. Defaults to end of times.
    std::chrono::time_point<std::chrono::steady_clock> nextTimeout=decltype(nextTimeout)::max();
    /// A extra state variable used in the return value of fork()
    ForkState forkState = ForkState::notForking; ///<
  };
  /** Noop function returning status. */
  virtual ProcessingStatus getInitialStatus() = 0;
  /** Function called to process events, as the object's address comes out of epoll. */
  virtual ProcessingStatus processEvent() = 0;
  /** Function called to process timeouts. */
  virtual ProcessingStatus processTimeout() = 0;
  /** Function called when SIGCHLD is received */
  virtual ProcessingStatus processSigChild() = 0;
  /** Function called when a subprocess requests the logger to be refreshed in all subprocesses */
  virtual ProcessingStatus processRefreshLoggerRequest() = 0;
  /** Function called to refreshed the logger of a subprocess */
  virtual ProcessingStatus refreshLogger() = 0;
  /** Instructs the handler to initiate a clean shutdown and update its status */
  virtual ProcessingStatus shutdown() = 0;
  /** Instructs the handler to kill its subprocess immediately. The process
   * should be gone upon return. */
  virtual void kill() = 0;
  /** Cleanup anything that should be in the child process */
  virtual void postForkCleanup() = 0;
  /** Instructs the handler to proceed with the fork it requested (returns
   * which side of the fork we are in) */
  virtual ProcessingStatus fork() = 0;
  /** Instructs the handler to run the intended function. We are in the new
   * process and it's all his. This function should return the exit() code or
   * call exit() itself */
  virtual int runChild() = 0;
};

} // namespace cta::tape::daemon
